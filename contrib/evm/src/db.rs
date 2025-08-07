use core::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use num_bigint::BigInt;
use psl::consensus::batch_proposal::MsgAckChanWithTag;
use psl::proto::client::ProtoClientReply;
use psl::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase};
use psl::rpc::server::LatencyProfile;
use psl::rpc::{PinnedMessage, SenderType};
use psl::utils::channel::{make_channel, Receiver, Sender};
use psl::worker::block_sequencer::BlockSeqNumQuery;
use psl::worker::cache_manager::{CacheCommand, CacheKey};
use revm::database::WrapDatabaseAsync;
use revm::database_interface::{async_db::{DatabaseAsync, DatabaseAsyncRef},
    Database, DatabaseCommit, DatabaseRef, EmptyDB, BENCH_CALLER, BENCH_CALLER_BALANCE,
    BENCH_TARGET, BENCH_TARGET_BALANCE,
};
use revm::primitives::{
    hash_map::Entry, Address, HashMap, Log, StorageKey, StorageValue, B256, KECCAK_EMPTY, U256,
};
use revm::state::{Account, AccountInfo, Bytecode};
use serde::Deserialize;
use tokio::sync::oneshot;
use std::vec::Vec;
use prost::Message as _;
use log::info;


pub struct PslKvDb {
    pub db_chan: Option<Sender<(Option<ProtoTransaction>, MsgAckChanWithTag)>>,

    pub client_tag: AtomicU64,
}

impl PslKvDb {
    pub fn new() -> Self {
        Self { db_chan: None, client_tag: AtomicU64::new(1) }
    }

    pub fn init(&mut self) -> Receiver<(Option<ProtoTransaction>, MsgAckChanWithTag)> {
        let (tx, rx) = make_channel(1000);
        self.db_chan = Some(tx);
        rx
    }

    fn make_read_transaction(key: CacheKey) -> ProtoTransaction {
        ProtoTransaction {
            is_2pc: false,
            is_reconfiguration: false,
            on_byzantine_commit: None,
            on_crash_commit: None,
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Read as i32,
                    operands: vec![key],
                }],
            }),
        }
    }

    fn make_write_transaction(key: CacheKey, value: Vec<u8>) -> ProtoTransaction {
        ProtoTransaction {
            is_2pc: false,
            is_reconfiguration: false,
            on_byzantine_commit: None,
            on_crash_commit: None,
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Write as i32,
                    operands: vec![key, value],
                }],
            }),
        }
    }

    fn deserialize_response(response: PinnedMessage) -> Vec<Vec<u8>> {
        let reply = ProtoClientReply::decode(&response.as_ref().0.as_slice()[0..response.as_ref().1]);
        let Ok(reply) = reply else {
            return vec![];
        };

        let Some(reply) = reply.reply else {
            return vec![];
        };

        let psl::proto::client::proto_client_reply::Reply::Receipt(reply) = reply else {
            return vec![];
        };

        let Some(result) = reply.results else {
            return vec![];
        };


        let Some(result) = result.result.first() else {
            return vec![];
        };
        
        result.values.iter().map(|v| v.clone()).collect()
        
    }

    // pub async fn run(mut rx: Receiver<CacheCommand>) {
    //     while let Some(cmd) = rx.recv().await {
    //         match cmd {
    //             CacheCommand::Get(key, sender) => {
    //                 let op = Self::make_read_transaction(key);
    //                 let (tx, rx) = oneshot::channel();
    //             }
    //             CacheCommand::Put(key, value, big_int, block_seq_num_query, sender) => {
    //                 cache.insert(key, value);
    //                 let _ = sender.send(Ok(0));
    //             },
    //             CacheCommand::Cas(key, value, _, sender) => todo!(),
    //             CacheCommand::Commit => todo!(),
    //         }
    //     }
    // }

    async fn get_key(&self, key: CacheKey) -> Result<Vec<u8>, Infallible> {
        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(1);
        self.db_chan.as_ref().unwrap().send((Some(Self::make_read_transaction(key)), (ack_tx, self.client_tag.fetch_add(1, Ordering::Relaxed), SenderType::Auth("node1".to_string(), 0)))).await.unwrap();
        let res = ack_rx.recv().await.unwrap();
        let res = Self::deserialize_response(res.0);
        Ok(res.first().unwrap().clone())
    }

    async fn put_key(&self, key: CacheKey, value: Vec<u8>) -> Result<(), Infallible> {
        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(1);
        self.db_chan.as_ref().unwrap().send((Some(Self::make_write_transaction(key, value)), (ack_tx, self.client_tag.fetch_add(1, Ordering::Relaxed), SenderType::Auth("node1".to_string(), 0)))).await.unwrap();
        info!("Sent write transaction");
        let _res = ack_rx.recv().await.unwrap();
        info!("Received write transaction");
        Ok(())
    }

    async fn get_account(&self, address: Address) -> Result<DbAccount, Infallible> {
        let key = format!("account:{}", address);
        let serialized_account = self.get_key(key.as_bytes().to_vec()).await?;
        let account = serde_json::from_slice(&serialized_account)
            .unwrap_or_else(|_| DbAccount::default());
        Ok(account)
    }    

    async fn put_account(&self, address: Address, account: DbAccount) -> Result<(), Infallible> {
        let key = format!("account:{}", address);
        let serialized_account = serde_json::to_vec(&account).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_account).await
    }

    async fn get_contract(&self, code_hash: B256) -> Result<Bytecode, Infallible> {
        let key = format!("contract:{}", code_hash);
        let serialized_contract = self.get_key(key.as_bytes().to_vec()).await?;
        let contract = serde_json::from_slice(&serialized_contract)
            .unwrap_or_else(|_| Bytecode::default());
        Ok(contract)
    }

    async fn put_contract(&self, code_hash: B256, contract: Bytecode) -> Result<(), Infallible> {
        let key = format!("contract:{}", code_hash);
        let serialized_contract = serde_json::to_vec(&contract).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_contract).await
    }

    async fn get_block_hash(&self, number: u64) -> Result<B256, Infallible> {
        let key = format!("block_hash:{}", number);
        let serialized_block_hash = self.get_key(key.as_bytes().to_vec()).await?;
        let block_hash = serde_json::from_slice(&serialized_block_hash)
            .unwrap_or_else(|_| B256::default());
        Ok(block_hash)
    }

    async fn put_block_hash(&self, number: u64, block_hash: B256) -> Result<(), Infallible> {
        let key = format!("block_hash:{}", number);
        let serialized_block_hash = serde_json::to_vec(&block_hash).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_block_hash).await
    }


    // Must call put_account with the DbAccount after this.
    pub async fn insert_contract(&mut self, account: &mut AccountInfo) {
        if let Some(code) = &account.code {
            if !code.is_empty() {
                if account.code_hash == KECCAK_EMPTY {
                    account.code_hash = code.hash_slow();
                }
                self.put_contract(account.code_hash, code.clone()).await.unwrap();
            }
        }
        if account.code_hash.is_zero() {
            account.code_hash = KECCAK_EMPTY;
        }
    }

    pub async fn insert_account_info(&mut self, address: Address, account: AccountInfo) {
        self.put_account(address, DbAccount {
            info: account,
            account_state: AccountState::None,
            storage: HashMap::new(),
        }).await.unwrap();
    }

    pub async fn load_account(&self, address: Address) -> Result<DbAccount, Infallible> {
        self.get_account(address).await
    }

}

// /// A [Database] implementation that stores all state changes in memory.
// pub type InMemoryDB = CacheDB<EmptyDB>;

// /// A cache used in [CacheDB]. Its kept separate so it can be used independently.
// ///
// /// Accounts and code are stored in two separate maps, the `accounts` map maps addresses to [DbAccount],
// /// whereas contracts are identified by their code hash, and are stored in the `contracts` map.
// /// The [DbAccount] holds the code hash of the contract, which is used to look up the contract in the `contracts` map.
// #[derive(Debug, Clone)]
// #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub struct Cache {
//     /// Account info where None means it is not existing. Not existing state is needed for Pre TANGERINE forks.
//     /// `code` is always `None`, and bytecode can be found in `contracts`.
//     pub accounts: HashMap<Address, DbAccount>,
//     /// Tracks all contracts by their code hash.
//     pub contracts: HashMap<B256, Bytecode>,
//     /// All logs that were committed via [DatabaseCommit::commit].
//     pub logs: Vec<Log>,
//     /// All cached block hashes from the [DatabaseRef].
//     pub block_hashes: HashMap<U256, B256>,
// }

// impl Default for Cache {
//     fn default() -> Self {
//         let mut contracts = HashMap::default();
//         contracts.insert(KECCAK_EMPTY, Bytecode::default());
//         contracts.insert(B256::ZERO, Bytecode::default());

//         Cache {
//             accounts: HashMap::default(),
//             contracts,
//             logs: Vec::default(),
//             block_hashes: HashMap::default(),
//         }
//     }
// }

// /// A [Database] implementation that stores all state changes in memory.
// ///
// /// This implementation wraps a [DatabaseRef] that is used to load data ([AccountInfo]).
// #[derive(Debug, Clone)]
// #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub struct CacheDB<ExtDB> {
//     /// The cache that stores all state changes.
//     pub cache: Cache,
//     /// The underlying database ([DatabaseRef]) that is used to load data.
//     ///
//     /// Note: This is read-only, data is never written to this database.
//     pub db: ExtDB,
// }

// impl<ExtDB: Default> Default for CacheDB<ExtDB> {
//     fn default() -> Self {
//         Self::new(ExtDB::default())
//     }
// }

// impl<ExtDb> CacheDB<CacheDB<ExtDb>> {
//     /// Flattens a nested cache by applying the outer cache to the inner cache.
//     ///
//     /// The behavior is as follows:
//     /// - Accounts are overridden with outer accounts
//     /// - Contracts are overridden with outer contracts
//     /// - Logs are appended
//     /// - Block hashes are overridden with outer block hashes
//     pub fn flatten(self) -> CacheDB<ExtDb> {
//         let CacheDB {
//             cache:
//                 Cache {
//                     accounts,
//                     contracts,
//                     logs,
//                     block_hashes,
//                 },
//             db: mut inner,
//         } = self;

//         inner.cache.accounts.extend(accounts);
//         inner.cache.contracts.extend(contracts);
//         inner.cache.logs.extend(logs);
//         inner.cache.block_hashes.extend(block_hashes);
//         inner
//     }

//     /// Discards the outer cache and return the inner cache.
//     pub fn discard_outer(self) -> CacheDB<ExtDb> {
//         self.db
//     }
// }

// impl<ExtDB> CacheDB<ExtDB> {
//     /// Creates a new cache with the given external database.
//     pub fn new(db: ExtDB) -> Self {
//         Self {
//             cache: Cache::default(),
//             db,
//         }
//     }

//     /// Inserts the account's code into the cache.
//     ///
//     /// Accounts objects and code are stored separately in the cache, this will take the code from the account and instead map it to the code hash.
//     ///
//     /// Note: This will not insert into the underlying external database.
//     pub fn insert_contract(&mut self, account: &mut AccountInfo) {
//         if let Some(code) = &account.code {
//             if !code.is_empty() {
//                 if account.code_hash == KECCAK_EMPTY {
//                     account.code_hash = code.hash_slow();
//                 }
//                 self.cache
//                     .contracts
//                     .entry(account.code_hash)
//                     .or_insert_with(|| code.clone());
//             }
//         }
//         if account.code_hash.is_zero() {
//             account.code_hash = KECCAK_EMPTY;
//         }
//     }

//     /// Inserts account info but not override storage
//     pub fn insert_account_info(&mut self, address: Address, mut info: AccountInfo) {
//         self.insert_contract(&mut info);
//         let account_entry = self.cache.accounts.entry(address).or_default();
//         account_entry.update_info(info);
//         if account_entry.account_state == AccountState::NotExisting {
//             account_entry.update_account_state(AccountState::None);
//         }
//     }

//     /// Wraps the cache in a [CacheDB], creating a nested cache.
//     pub fn nest(self) -> CacheDB<Self> {
//         CacheDB::new(self)
//     }
// }

// impl<ExtDB: DatabaseRef> CacheDB<ExtDB> {
//     /// Returns the account for the given address.
//     ///
//     /// If the account was not found in the cache, it will be loaded from the underlying database.
//     pub fn load_account(&mut self, address: Address) -> Result<&mut DbAccount, ExtDB::Error> {
//         let db = &self.db;
//         match self.cache.accounts.entry(address) {
//             Entry::Occupied(entry) => Ok(entry.into_mut()),
//             Entry::Vacant(entry) => Ok(entry.insert(
//                 db.basic_ref(address)?
//                     .map(|info| DbAccount {
//                         info,
//                         ..Default::default()
//                     })
//                     .unwrap_or_else(DbAccount::new_not_existing),
//             )),
//         }
//     }

//     /// Inserts account storage without overriding account info
//     pub fn insert_account_storage(
//         &mut self,
//         address: Address,
//         slot: StorageKey,
//         value: StorageValue,
//     ) -> Result<(), ExtDB::Error> {
//         let account = self.load_account(address)?;
//         account.storage.insert(slot, value);
//         Ok(())
//     }

//     /// Replaces account storage without overriding account info
//     pub fn replace_account_storage(
//         &mut self,
//         address: Address,
//         storage: HashMap<StorageKey, StorageValue>,
//     ) -> Result<(), ExtDB::Error> {
//         let account = self.load_account(address)?;
//         account.account_state = AccountState::StorageCleared;
//         account.storage = storage.into_iter().collect();
//         Ok(())
//     }
// }

struct HandleMod(tokio::runtime::Handle);

impl HandleMod {
    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        tokio::task::block_in_place(move || self.0.block_on(f))
    }
}

impl DatabaseCommit for PslKvDb {
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        let rt = HandleMod(tokio::runtime::Handle::current());
        for (address, mut account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                let mut db_account = rt.block_on(self.get_account(address)).unwrap();
                db_account.storage.clear();
                db_account.account_state = AccountState::NotExisting;
                db_account.info = AccountInfo::default();
                rt.block_on(self.put_account(address, db_account)).unwrap();
                continue;
            }
            let is_newly_created = account.is_created();
            rt.block_on(self.insert_contract(&mut account.info));

            let mut db_account = rt.block_on(self.get_account(address)).unwrap();
            db_account.info = account.info;

            db_account.account_state = if is_newly_created {
                db_account.storage.clear();
                AccountState::StorageCleared
            } else if db_account.account_state.is_storage_cleared() {
                // Preserve old account state if it already exists
                AccountState::StorageCleared
            } else {
                AccountState::Touched
            };
            db_account.storage.extend(
                account
                    .storage
                    .into_iter()
                    .map(|(key, value)| (key, value.present_value())),
            );
            rt.block_on(self.put_account(address, db_account)).unwrap();
        }
    }
}

impl DatabaseAsync for PslKvDb {
// impl Database for PslKvDb {
    type Error = Infallible;

    async fn basic_async(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let basic = self.get_account(address).await.unwrap();
        Ok(basic.info())
    }

    async fn code_by_hash_async(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let contract = self.get_contract(code_hash).await.unwrap();
        Ok(contract)
    }

    /// Get the value in an account's storage slot.
    ///
    /// It is assumed that account is already loaded.
    async fn storage_async(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let mut acc_entry = self.get_account(address).await.unwrap();
        match acc_entry.storage.entry(index) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(_) => Ok(StorageValue::ZERO)
        }
    }

    async fn block_hash_async(&mut self, number: u64) -> Result<B256, Self::Error> {
        let block_hash = self.get_block_hash(number).await.unwrap();
        Ok(block_hash)
    }
}

impl DatabaseAsyncRef for PslKvDb {
    type Error = Infallible;

    async fn basic_async_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self.get_account(address).await.unwrap();
        Ok(account.info())
    }

    async fn code_by_hash_async_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let contract = self.get_contract(code_hash).await.unwrap();
        Ok(contract)
    }

    async fn storage_async_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let account = self.get_account(address).await.unwrap();
        match account.storage.get(&index) {
            Some(entry) => Ok(*entry),
            None => {
                Ok(StorageValue::ZERO)
            }
        }
    }

    async fn block_hash_async_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let block_hash = self.get_block_hash(number).await.unwrap();
        Ok(block_hash)
    }
}

impl Database for PslKvDb {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let basic = rt.block_on(self.get_account(address)).unwrap();
        Ok(basic.info())
    }
    
    #[doc = " Gets account code by its hash."]
    fn code_by_hash(&mut self,code_hash:B256) -> Result<Bytecode,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let contract = rt.block_on(self.get_contract(code_hash)).unwrap();
        Ok(contract)
    }
    
    #[doc = " Gets storage value of address at index."]
    fn storage(&mut self,address:Address,index:StorageKey) -> Result<StorageValue,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let storage = rt.block_on(self.storage_async(address, index)).unwrap();
        Ok(storage)
    }
    
    #[doc = " Gets block hash by block number."]
    fn block_hash(&mut self,number:u64) -> Result<B256,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let block_hash = rt.block_on(self.get_block_hash(number)).unwrap();
        Ok(block_hash)
    }
}

impl DatabaseRef for PslKvDb {
    type Error = Infallible;
    
    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address:Address) -> Result<Option<AccountInfo> ,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let account = rt.block_on(self.get_account(address)).unwrap();
        Ok(account.info())
    }
    
    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash:B256) -> Result<Bytecode,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let contract = rt.block_on(self.get_contract(code_hash)).unwrap();
        Ok(contract)
    }
    
    #[doc = " Gets storage value of address at index."]
    fn storage_ref(&self, address:Address, index:StorageKey) -> Result<StorageValue,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let storage = rt.block_on(self.storage_async_ref(address, index)).unwrap();
        Ok(storage)
    }
    
    #[doc = " Gets block hash by block number."]
    fn block_hash_ref(&self, number:u64) -> Result<B256,Self::Error>  {
        let rt = HandleMod(tokio::runtime::Handle::current());
        let block_hash = rt.block_on(self.get_block_hash(number)).unwrap();
        Ok(block_hash)
    }
}

/// Database account representation.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DbAccount {
    /// Basic account information.
    pub info: AccountInfo,
    /// If account is selfdestructed or newly created, storage will be cleared.
    pub account_state: AccountState,
    /// Storage slots
    pub storage: HashMap<StorageKey, StorageValue>,
}

impl DbAccount {
    /// Creates a new non-existing account.
    pub fn new_not_existing() -> Self {
        Self {
            account_state: AccountState::NotExisting,
            ..Default::default()
        }
    }

    /// Returns account info if the account exists.
    pub fn info(&self) -> Option<AccountInfo> {
        if matches!(self.account_state, AccountState::NotExisting) {
            None
        } else {
            Some(self.info.clone())
        }
    }

    /// Updates the account information.
    #[inline(always)]
    pub fn update_info(&mut self, info: AccountInfo) {
        self.info = info;
    }

    /// Updates the account state.
    #[inline(always)]
    pub fn update_account_state(&mut self, account_state: AccountState) {
        self.account_state = account_state;
    }
}

impl From<Option<AccountInfo>> for DbAccount {
    fn from(from: Option<AccountInfo>) -> Self {
        from.map(Self::from).unwrap_or_else(Self::new_not_existing)
    }
}

impl From<AccountInfo> for DbAccount {
    fn from(info: AccountInfo) -> Self {
        Self {
            info,
            account_state: AccountState::None,
            ..Default::default()
        }
    }
}

/// State of an account in the database.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AccountState {
    /// Before Spurious Dragon hardfork there was a difference between empty and not existing.
    /// And we are flagging it here.
    NotExisting,
    /// EVM touched this account. For newer hardfork this means it can be cleared/removed from state.
    Touched,
    /// EVM cleared storage of this account, mostly by selfdestruct, we don't ask database for storage slots
    /// and assume they are StorageValue::ZERO
    StorageCleared,
    /// EVM didn't interacted with this account
    #[default]
    None,
}

impl AccountState {
    /// Returns `true` if EVM cleared storage of this account
    pub fn is_storage_cleared(&self) -> bool {
        matches!(self, AccountState::StorageCleared)
    }
}

/// Custom benchmarking DB that only has account info for the zero address.
///
/// Any other address will return an empty account.
#[derive(Debug, Default, Clone)]
pub struct BenchmarkDB(pub Bytecode, B256);

impl BenchmarkDB {
    /// Creates a new benchmark database with the given bytecode.
    pub fn new_bytecode(bytecode: Bytecode) -> Self {
        let hash = bytecode.hash_slow();
        Self(bytecode, hash)
    }
}

impl Database for BenchmarkDB {
    type Error = Infallible;
    /// Get basic account information.
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if address == BENCH_TARGET {
            return Ok(Some(AccountInfo {
                nonce: 1,
                balance: BENCH_TARGET_BALANCE,
                code: Some(self.0.clone()),
                code_hash: self.1,
            }));
        }
        if address == BENCH_CALLER {
            return Ok(Some(AccountInfo {
                nonce: 0,
                balance: BENCH_CALLER_BALANCE,
                code: None,
                code_hash: KECCAK_EMPTY,
            }));
        }
        Ok(None)
    }

    /// Get account code by its hash
    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::default())
    }

    /// Get storage value of address at index.
    fn storage(
        &mut self,
        _address: Address,
        _index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        Ok(StorageValue::default())
    }

    // History related
    fn block_hash(&mut self, _number: u64) -> Result<B256, Self::Error> {
        Ok(B256::default())
    }
}
