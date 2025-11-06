//! `eth_` `PubSub` RPC handler implementation

use std::{collections::HashMap, sync::Arc};
use alloy_consensus::{transaction::Recovered, Transaction};
use alloy_eips::Typed2718;
use alloy_primitives::{b256, uint, Address, TxHash, B256, U256};
use alloy_rpc_types_eth::{
    pubsub::{Params, PubSubSyncStatus, SubscriptionKind, SyncStatusMetadata},
    BlockId, BlockNumberOrTag, Filter, Header, Log,
};
use futures::StreamExt;
use jsonrpsee::{
    server::SubscriptionMessage, types::ErrorObject, PendingSubscriptionSink, SubscriptionSink,
};
use reth_chain_state::CanonStateSubscriptions;
use reth_evm::{ConfigureEvm, Evm};
use reth_network_api::NetworkInfo;
use reth_primitives_traits::{NodePrimitives, SignedTransaction, TxTy};
use reth_revm::{database::StateProviderDatabase, db::CacheDB};
use reth_rpc_eth_api::{
    helpers::Call, pubsub::EthPubSubApiServer, EthApiTypes, FromEvmError, RpcConvert, RpcNodeCore,
    RpcTransaction,
};
use reth_rpc_eth_types::logs_utils;
use reth_rpc_server_types::result::{internal_rpc_err, invalid_params_rpc_err};
use reth_storage_api::BlockNumReader;
use reth_tasks::{TaskSpawner, TokioTaskExecutor};
use reth_transaction_pool::{NewTransactionEvent, PoolConsensusTx, TransactionPool};
use revm::{context::result::ResultAndState, context_interface::result::ExecutionResult};
use serde::Serialize;
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream,
};
use tracing::error;

const UNISWAP_V2_SYNC_TOPIC: B256 =
    b256!("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1");
const UNISWAP_V3_SWAP_TOPIC: B256 =
    b256!("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");
const PANCAKE_V3_SWAP_TOPIC: B256 =
    b256!("0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83");
const UNISWAP_V4_SWAP_TOPIC: B256 =
    b256!("0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f");
const INFINITY_SWAP_TOPIC: B256 =
    b256!("0x04206ad2b7c0f463bff3dd4f33c5735b0f2957a351e4f79763a4fa9e775dd237");

/// `Eth` pubsub RPC implementation.
///
/// This handles `eth_subscribe` RPC calls.
#[derive(Clone)]
pub struct EthPubSub<Eth> {
    /// All nested fields bundled together.
    inner: Arc<EthPubSubInner<Eth>>,
}

// === impl EthPubSub ===

impl<Eth> EthPubSub<Eth> {
    /// Creates a new, shareable instance.
    ///
    /// Subscription tasks are spawned via [`tokio::task::spawn`]
    pub fn new(eth_api: Eth) -> Self {
        Self::with_spawner(eth_api, Box::<TokioTaskExecutor>::default())
    }

    /// Creates a new, shareable instance.
    pub fn with_spawner(eth_api: Eth, subscription_task_spawner: Box<dyn TaskSpawner>) -> Self {
        let inner = EthPubSubInner { eth_api, subscription_task_spawner };
        Self { inner: Arc::new(inner) }
    }
}

impl<N: NodePrimitives, Eth> EthPubSub<Eth>
where
    Eth: RpcNodeCore<
            Provider: BlockNumReader + CanonStateSubscriptions<Primitives = N>,
            Pool: TransactionPool,
            Network: NetworkInfo,
        > + EthApiTypes<
            RpcConvert: RpcConvert<
                Primitives: NodePrimitives<SignedTx = PoolConsensusTx<Eth::Pool>>,
            >,
        > + Call,
{
    /// Returns the current sync status for the `syncing` subscription
    pub fn sync_status(&self, is_syncing: bool) -> PubSubSyncStatus {
        self.inner.sync_status(is_syncing)
    }

    /// Returns a stream that yields all transaction hashes emitted by the txpool.
    pub fn pending_transaction_hashes_stream(&self) -> impl Stream<Item = TxHash> {
        self.inner.pending_transaction_hashes_stream()
    }

    /// Returns a stream that yields all transactions emitted by the txpool.
    pub fn full_pending_transaction_stream(
        &self,
    ) -> impl Stream<Item = NewTransactionEvent<<Eth::Pool as TransactionPool>::Transaction>> {
        self.inner.full_pending_transaction_stream()
    }

    /// Returns a stream that yields all new RPC blocks.
    pub fn new_headers_stream(&self) -> impl Stream<Item = Header<N::BlockHeader>> {
        self.inner.new_headers_stream()
    }

    /// Returns a stream that yields all logs that match the given filter.
    pub fn log_stream(&self, filter: Filter) -> impl Stream<Item = Vec<Log>> {
        self.inner.log_stream(filter)
    }

    /// The actual handler for an accepted [`EthPubSub::subscribe`] call.
    pub async fn handle_accepted(
        &self,
        accepted_sink: SubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> Result<(), ErrorObject<'static>> {
        match kind {
            SubscriptionKind::NewHeads => {
                pipe_from_stream(accepted_sink, self.new_headers_stream()).await
            }
            SubscriptionKind::Logs => {
                // if no params are provided, used default filter params
                let filter = match params {
                    Some(Params::Logs(filter)) => *filter,
                    Some(Params::Bool(_)) => {
                        return Err(invalid_params_rpc_err("Invalid params for logs"))
                    }
                    _ => Default::default(),
                };
                pipe_from_stream(accepted_sink, self.log_stream(filter)).await
            }
            SubscriptionKind::NewPendingTransactions => {
                if let Some(params) = params {
                    match params {
                        Params::Bool(true) => {
                            // full transaction objects requested
                            let inner = self.inner.clone();
                            let stream = self
                                .full_pending_transaction_stream()
                                .filter_map(move |tx| {
                                    let inner = inner.clone();
                                    async move {
                                        inner
                                            .simulate_tx(tx.transaction.to_consensus())
                                            .await
                                            .ok()?
                                    }
                                })
                                .boxed();
                            return pipe_from_stream(accepted_sink, stream).await;
                        }
                        Params::Bool(false) | Params::None => {
                            // only hashes requested
                        }
                        Params::Logs(_) => {
                            return Err(invalid_params_rpc_err(
                                "Invalid params for newPendingTransactions",
                            ))
                        }
                    }
                }

                pipe_from_stream(accepted_sink, self.pending_transaction_hashes_stream()).await
            }
            SubscriptionKind::Syncing => {
                // get new block subscription
                let mut canon_state = BroadcastStream::new(
                    self.inner.eth_api.provider().subscribe_to_canonical_state(),
                );
                // get current sync status
                let mut initial_sync_status = self.inner.eth_api.network().is_syncing();
                let current_sub_res = self.sync_status(initial_sync_status);

                // send the current status immediately
                let msg = SubscriptionMessage::new(
                    accepted_sink.method_name(),
                    accepted_sink.subscription_id(),
                    &current_sub_res,
                )
                .map_err(SubscriptionSerializeError::new)?;

                if accepted_sink.send(msg).await.is_err() {
                    return Ok(());
                }

                while canon_state.next().await.is_some() {
                    let current_syncing = self.inner.eth_api.network().is_syncing();
                    // Only send a new response if the sync status has changed
                    if current_syncing != initial_sync_status {
                        // Update the sync status on each new block
                        initial_sync_status = current_syncing;

                        // send a new message now that the status changed
                        let sync_status = self.sync_status(current_syncing);
                        let msg = SubscriptionMessage::new(
                            accepted_sink.method_name(),
                            accepted_sink.subscription_id(),
                            &sync_status,
                        )
                        .map_err(SubscriptionSerializeError::new)?;

                        if accepted_sink.send(msg).await.is_err() {
                            break;
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

#[async_trait::async_trait]
impl<Eth> EthPubSubApiServer<RpcTransaction<Eth::NetworkTypes>> for EthPubSub<Eth>
where
    Eth: RpcNodeCore<
            Provider: BlockNumReader + CanonStateSubscriptions,
            Pool: TransactionPool,
            Network: NetworkInfo,
        > + EthApiTypes<
            RpcConvert: RpcConvert<
                Primitives: NodePrimitives<SignedTx = PoolConsensusTx<Eth::Pool>>,
            >,
        > + Call
        + 'static,
{
    /// Handler for `eth_subscribe`
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink = pending.accept().await?;
        let pubsub = self.clone();
        self.inner.subscription_task_spawner.spawn(Box::pin(async move {
            let _ = pubsub.handle_accepted(sink, kind, params).await;
        }));

        Ok(())
    }
}

/// Helper to convert a serde error into an [`ErrorObject`]
#[derive(Debug, thiserror::Error)]
#[error("Failed to serialize subscription item: {0}")]
pub struct SubscriptionSerializeError(#[from] serde_json::Error);

impl SubscriptionSerializeError {
    const fn new(err: serde_json::Error) -> Self {
        Self(err)
    }
}

impl From<SubscriptionSerializeError> for ErrorObject<'static> {
    fn from(value: SubscriptionSerializeError) -> Self {
        internal_rpc_err(value.to_string())
    }
}

/// Pipes all stream items to the subscription sink.
async fn pipe_from_stream<T, St>(
    sink: SubscriptionSink,
    mut stream: St,
) -> Result<(), ErrorObject<'static>>
where
    St: Stream<Item = T> + Unpin,
    T: Serialize,
{
    loop {
        tokio::select! {
            _ = sink.closed() => {
                // connection dropped
                break Ok(())
            },
            maybe_item = stream.next() => {
                let item = match maybe_item {
                    Some(item) => item,
                    None => {
                        // stream ended
                        break  Ok(())
                    },
                };
                let msg = SubscriptionMessage::new(
                    sink.method_name(),
                    sink.subscription_id(),
                    &item
                ).map_err(SubscriptionSerializeError::new)?;

                if sink.send(msg).await.is_err() {
                    break Ok(());
                }
            }
        }
    }
}

impl<Eth> std::fmt::Debug for EthPubSub<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthPubSub").finish_non_exhaustive()
    }
}

/// Container type `EthPubSub`
#[derive(Clone)]
struct EthPubSubInner<EthApi> {
    /// The `eth` API.
    eth_api: EthApi,
    /// The type that's used to spawn subscription tasks.
    subscription_task_spawner: Box<dyn TaskSpawner>,
}

// == impl EthPubSubInner ===

impl<Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Provider: BlockNumReader>,
{
    /// Returns the current sync status for the `syncing` subscription
    fn sync_status(&self, is_syncing: bool) -> PubSubSyncStatus {
        if is_syncing {
            let current_block = self
                .eth_api
                .provider()
                .chain_info()
                .map(|info| info.best_number)
                .unwrap_or_default();
            PubSubSyncStatus::Detailed(SyncStatusMetadata {
                syncing: true,
                starting_block: 0,
                current_block,
                highest_block: Some(current_block),
            })
        } else {
            PubSubSyncStatus::Simple(false)
        }
    }
}

impl<Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Pool: TransactionPool>,
{
    /// Returns a stream that yields all transaction hashes emitted by the txpool.
    fn pending_transaction_hashes_stream(&self) -> impl Stream<Item = TxHash> {
        ReceiverStream::new(self.eth_api.pool().pending_transactions_listener())
    }

    /// Returns a stream that yields all transactions emitted by the txpool.
    fn full_pending_transaction_stream(
        &self,
    ) -> impl Stream<Item = NewTransactionEvent<<Eth::Pool as TransactionPool>::Transaction>> {
        self.eth_api.pool().new_pending_pool_transactions_listener()
    }
}

impl<N: NodePrimitives, Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Provider: CanonStateSubscriptions<Primitives = N>>,
{
    /// Returns a stream that yields all new RPC blocks.
    fn new_headers_stream(&self) -> impl Stream<Item = Header<N::BlockHeader>> {
        self.eth_api.provider().canonical_state_stream().flat_map(|new_chain| {
            let headers = new_chain
                .committed()
                .blocks_iter()
                .map(|block| {
                    Header::from_consensus(
                        block.clone_sealed_header().into(),
                        None,
                        Some(U256::from(block.rlp_length())),
                    )
                })
                .collect::<Vec<_>>();
            futures::stream::iter(headers)
        })
    }

    /// Returns a stream that yields all logs that match the given filter.
    fn log_stream(&self, filter: Filter) -> impl Stream<Item = Vec<Log>> {
        BroadcastStream::new(self.eth_api.provider().subscribe_to_canonical_state())
            .map(move |canon_state| {
                canon_state.expect("new block subscription never ends").block_receipts()
            })
            .flat_map(futures::stream::iter)
            .map(move |(block_receipts, removed)| {
                let all_logs = logs_utils::matching_block_logs_with_tx_hashes(
                    &filter,
                    block_receipts.block,
                    block_receipts.timestamp,
                    block_receipts.tx_receipts.iter().map(|(tx, receipt)| (*tx, receipt)),
                    removed,
                );
                // mev hack, filter logs to only keep the latest event per type
                filter_logs(all_logs)
            })
    }
}


/// Structure representing simulated pending transaction with logs
#[derive(Serialize)]
struct SimulatedTransactionLogs<Tx> {
    tx: Tx,
    logs: Vec<Log>,
}

impl<Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Pool: TransactionPool>
        + EthApiTypes<
            RpcConvert: RpcConvert<
                Primitives: NodePrimitives<SignedTx = PoolConsensusTx<Eth::Pool>>,
            >,
        > + Call,
{
    /// Executes the transaction in a call-style context and gathers the emitted logs, if any.
    async fn simulate_tx_logs(
        &self,
        tx: Recovered<TxTy<Eth::Primitives>>,
    ) -> Result<Option<Vec<Log>>, Eth::Error> {
        let eth_api = self.eth_api.clone();
        let tx_hash = *tx.tx_hash();

        // Return early if the transaction has no input data or is EIP-4844
        if tx.input().is_empty() || tx.is_eip4844() {
            return Ok(None);
        }

        // Prepare EVM environment at latest block
        let (mut evm_env, at) =
            eth_api.evm_env_at(BlockId::Number(BlockNumberOrTag::Latest)).await?;
        evm_env.block_env.timestamp += uint!(1_U256);

        // Simulate the transaction
        let tx_for_exec = tx.clone();
        let logs = self
            .eth_api
            .clone()
            .spawn_with_state_at_block(at, move |state| {
                let db = CacheDB::new(StateProviderDatabase::new(state));
                let mut evm = eth_api.evm_config().evm_with_env(db, evm_env);
                let ResultAndState { result, .. } = evm
                    .transact(eth_api.evm_config().tx_env(&tx_for_exec))
                    .map_err(Eth::Error::from_evm_err)?;

                let logs = match result {
                    ExecutionResult::Success { logs, .. } => logs,
                    other => {
                        let _ = reth_rpc_eth_types::error::ensure_success::<_, Eth::Error>(other)?;
                        unreachable!("ensure_success returns Err for non-success outcomes")
                    }
                };
                Ok(logs)
            })
            .await?;

        // Return early if no logs were emitted
        if logs.is_empty() {
            return Ok(None);
        }

        // Map the logs to RPC logs
        let logs = logs
            .into_iter()
            .enumerate()
            .map(|(index, log)| Log {
                inner: log,
                transaction_hash: Some(tx_hash),
                transaction_index: None,
                log_index: Some(index as u64),
                removed: false,
                block_hash: None,
                block_number: None,
                block_timestamp: None,
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Ok(Some(logs))
    }

    /// simulates a transaction to get logs
    async fn simulate_tx(
        &self,
        tx: Recovered<TxTy<Eth::Primitives>>,
    ) -> Result<Option<SimulatedTransactionLogs<RpcTransaction<Eth::NetworkTypes>>>, Eth::Error>
    {
        let rpc_tx = self.eth_api.tx_resp_builder().fill_pending(tx.clone())?;
        let logs = match self.simulate_tx_logs(tx).await? {
            Some(logs) => filter_logs(logs),
            None => return Ok(None),
        };
        Ok(Some(SimulatedTransactionLogs { tx: rpc_tx, logs }))
    }
}

/// filters logs to only keep the latest event per type.
///
/// this function assume logs are ordered from oldest to newest
fn filter_logs(logs: Vec<Log>) -> Vec<Log> {
    let mut latest: HashMap<(Address, Option<B256>, Option<B256>), Log> = HashMap::new();
    for log in logs.into_iter() {
        let topics = &log.inner.topics();
        if topics.is_empty() {
            continue;
        }
        let sig = topics[0];
        let address = log.inner.address;
        let key = match sig {
            UNISWAP_V2_SYNC_TOPIC => (address, None, None),
            UNISWAP_V3_SWAP_TOPIC | PANCAKE_V3_SWAP_TOPIC => (address, Some(sig), None),
            UNISWAP_V4_SWAP_TOPIC | INFINITY_SWAP_TOPIC => {
                (address, Some(sig), topics.get(1).copied())
            }
            _ => continue,
        };
        latest.insert(key, log);
    }
    latest.into_values().collect()
}
