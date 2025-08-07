use core::convert::Infallible;
use num_bigint::BigInt;
use psl::utils::channel::{make_channel, Receiver, Sender};
use psl::worker::block_sequencer::BlockSeqNumQuery;
use psl::worker::cache_manager::{CacheCommand, CacheKey};
use revm::database_interface::{
    Database, DatabaseCommit, DatabaseRef, EmptyDB, BENCH_CALLER, BENCH_CALLER_BALANCE,
    BENCH_TARGET, BENCH_TARGET_BALANCE,
};
use revm::primitives::{
    hash_map::Entry, Address, HashMap, Log, StorageKey, StorageValue, B256, KECCAK_EMPTY, U256,
};
use revm::state::{Account, AccountInfo, Bytecode};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use std::vec::Vec;


pub struct PslKvDb {
    db_chan: Option<UnboundedSender<CacheCommand>>,
}

impl PslKvDb {
    pub fn new() -> Self {
        Self { db_chan: None }
    }

    pub fn init(&mut self) -> UnboundedReceiver<CacheCommand> {
        let (tx, rx) = unbounded_channel();
        self.db_chan = Some(tx);
        rx
    }

    pub async fn run(mut rx: UnboundedReceiver<CacheCommand>) {
        let mut cache = HashMap::<CacheKey, Vec<u8>>::new();
        while let Some(cmd) = rx.recv().await {
            match cmd {
                CacheCommand::Get(key, sender) => {
                    let value = cache.get(&key);
                    if let Some(value) = value {
                        let _ = sender.send(Ok((value.clone(), 0)));
                    } else {
                        let _ = sender.send(Ok((vec![], 0)));
                    }
                }
                CacheCommand::Put(key, value, big_int, block_seq_num_query, sender) => {
                    cache.insert(key, value);
                    let _ = sender.send(Ok(0));
                },
                CacheCommand::Cas(key, value, _, sender) => todo!(),
                CacheCommand::Commit => todo!(),
            }
        }
    }

    fn get_key(&self, key: CacheKey) -> Result<Vec<u8>, Infallible> {
        let (tx, rx) = oneshot::channel();
        
        self.db_chan.as_ref().unwrap().send(CacheCommand::Get(key, tx)).unwrap();
        let res = rx.blocking_recv().unwrap().unwrap();
        Ok(res.0)
    }

    fn put_key(&self, key: CacheKey, value: Vec<u8>) -> Result<(), Infallible> {
        let (tx, rx) = oneshot::channel();
        self.db_chan.as_ref().unwrap().send(CacheCommand::Put(key, value, BigInt::from(0), BlockSeqNumQuery::DontBother, tx)).unwrap();
        let _ = rx.blocking_recv().unwrap().unwrap();
        Ok(())
    }

    fn get_account(&self, address: Address) -> Result<DbAccount, Infallible> {
        let key = format!("account:{}", address);
        let serialized_account = self.get_key(key.as_bytes().to_vec())?;
        let account = serde_json::from_slice(&serialized_account)
            .unwrap_or_else(|_| DbAccount::default());
        Ok(account)
    }    

    fn put_account(&self, address: Address, account: DbAccount) -> Result<(), Infallible> {
        let key = format!("account:{}", address);
        let serialized_account = serde_json::to_vec(&account).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_account)
    }

    fn get_contract(&self, code_hash: B256) -> Result<Bytecode, Infallible> {
        let key = format!("contract:{}", code_hash);
        let serialized_contract = self.get_key(key.as_bytes().to_vec())?;
        let contract = serde_json::from_slice(&serialized_contract)
            .unwrap_or_else(|_| Bytecode::default());
        Ok(contract)
    }

    fn put_contract(&self, code_hash: B256, contract: Bytecode) -> Result<(), Infallible> {
        let key = format!("contract:{}", code_hash);
        let serialized_contract = serde_json::to_vec(&contract).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_contract)
    }

    fn get_block_hash(&self, number: u64) -> Result<B256, Infallible> {
        let key = format!("block_hash:{}", number);
        let serialized_block_hash = self.get_key(key.as_bytes().to_vec())?;
        let block_hash = serde_json::from_slice(&serialized_block_hash)
            .unwrap_or_else(|_| B256::default());
        Ok(block_hash)
    }

    fn put_block_hash(&self, number: u64, block_hash: B256) -> Result<(), Infallible> {
        let key = format!("block_hash:{}", number);
        let serialized_block_hash = serde_json::to_vec(&block_hash).unwrap();
        self.put_key(key.as_bytes().to_vec(), serialized_block_hash)
    }


    // Must call put_account with the DbAccount after this.
    pub fn insert_contract(&mut self, account: &mut AccountInfo) {
        if let Some(code) = &account.code {
            if !code.is_empty() {
                if account.code_hash == KECCAK_EMPTY {
                    account.code_hash = code.hash_slow();
                }
                self.put_contract(account.code_hash, code.clone()).unwrap();
            }
        }
        if account.code_hash.is_zero() {
            account.code_hash = KECCAK_EMPTY;
        }
    }

    pub fn insert_account_info(&mut self, address: Address, account: AccountInfo) {
        self.put_account(address, DbAccount {
            info: account,
            account_state: AccountState::None,
            storage: HashMap::new(),
        }).unwrap();
    }

    pub fn load_account(&self, address: Address) -> Result<DbAccount, Infallible> {
        self.get_account(address)
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

impl DatabaseCommit for PslKvDb {
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        for (address, mut account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                let mut db_account = self.get_account(address).unwrap();
                db_account.storage.clear();
                db_account.account_state = AccountState::NotExisting;
                db_account.info = AccountInfo::default();
                self.put_account(address, db_account).unwrap();
                continue;
            }
            let is_newly_created = account.is_created();
            self.insert_contract(&mut account.info);

            let mut db_account = self.get_account(address).unwrap();
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
            self.put_account(address, db_account).unwrap();
        }
    }
}

impl Database for PslKvDb {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let basic = self.get_account(address).unwrap();
        Ok(basic.info())
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let contract = self.get_contract(code_hash).unwrap();
        Ok(contract)
    }

    /// Get the value in an account's storage slot.
    ///
    /// It is assumed that account is already loaded.
    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let mut acc_entry = self.get_account(address).unwrap();
        match acc_entry.storage.entry(index) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(_) => Ok(StorageValue::ZERO)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let block_hash = self.get_block_hash(number).unwrap();
        Ok(block_hash)
    }
}

impl DatabaseRef for PslKvDb {
    type Error = Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self.get_account(address).unwrap();
        Ok(account.info())
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let contract = self.get_contract(code_hash).unwrap();
        Ok(contract)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let account = self.get_account(address).unwrap();
        match account.storage.get(&index) {
            Some(entry) => Ok(*entry),
            None => {
                Ok(StorageValue::ZERO)
            }
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let block_hash = self.get_block_hash(number).unwrap();
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
