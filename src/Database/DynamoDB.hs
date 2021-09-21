{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

-- |
-- Module      : Data.DynamoDb
-- License     : BSD-style
--
-- Maintainer  : palkovsky.ondrej@gmail.com
-- Stability   : experimental
-- Portability : portable
--
-- Type-safe library for accessing DynamoDB database.
--
module Database.DynamoDB (
    -- * Introduction
    -- $intro

    -- * Proxies
    -- $proxy

    -- * Lens support
    -- $lens

    -- * Conversion support
    -- $conversion

    -- * Data types
    DynamoException(..)
  , Consistency(..)
  , Direction(..)
  , Column
    -- * Attribute path combinators
    , (<.>), (<!>), (<!:>)
    -- * Fetching items
  , getItem
  , getItemBatch
    -- * Query options
  , QueryOpts
  , queryOpts
  , qConsistentRead, qStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition, qLimit
    -- * Performing query
  , query
  , querySimple
  , queryCond
  , querySource
  , querySourceChunks
  , querySourceByKey
  , queryOverIndex
    -- * Scan options
  , ScanOpts
  , scanOpts
  , sFilterCondition, sConsistentRead, sLimit, sParallel, sStartKey
    -- * Performing scan
  , scan
  , scanSource
  , scanSourceChunks
  , scanCond
    -- * Helper conduits
  , leftJoin
  , innerJoin
    -- * Data entry
  , putItem
  , putItemBatch
  , insertItem
    -- * Data modification
  , updateItemByKey
  , updateItemByKey_
  , updateItemCond_
    -- * Deleting data
  , deleteItemByKey
  , deleteItemCondByKey
  , deleteItemBatchByKey
    -- * Delete table
  , deleteTable
    -- * Utility functions
  , tableKey
    -- * Typeclasses
  , DynamoTable
  , DynamoIndex
  , PrimaryKey
  , ContainsTableKey
  , CanQuery
  , TableScan
) where

import           Control.Lens                        ((%~), (.~), (?~), (^.))
import           Control.Monad                       (void)
import           Control.Monad.Catch                 (throwM)
import           Control.Monad.Trans.Resource
import           Data.Bool                           (bool)
import           Data.Function                       ((&))
import           Data.Maybe                          (fromMaybe)
import           Data.Proxy
import           Data.Semigroup                      ((<>))
import           Network.AWS
import qualified Network.AWS.DynamoDB.DeleteItem     as D
import qualified Network.AWS.DynamoDB.GetItem        as D
import qualified Network.AWS.DynamoDB.PutItem        as D
import qualified Network.AWS.DynamoDB.UpdateItem     as D
import qualified Network.AWS.DynamoDB.DeleteTable    as D
import qualified Network.AWS.DynamoDB.Types    as D

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types
import           Database.DynamoDB.Update
import           Database.DynamoDB.BatchRequest
import           Database.DynamoDB.QueryRequest


dDeleteItem :: DynamoTable a r => Proxy a -> PrimaryKey a r -> D.DeleteItem
dDeleteItem p pkey = D.newDeleteItem (tableName p) & D.deleteItem_key .~ dKeyToAttr p pkey

dGetItem :: DynamoTable a r => Proxy a -> PrimaryKey a r -> D.GetItem
dGetItem p pkey = D.newGetItem (tableName p) & D.getItem_key .~ dKeyToAttr p pkey

-- | Write item into the database; overwrite any previously existing item with the same primary key.
putItem :: (DynamoTable a r, MonadResource m, MonadThrow m) => Env -> a -> m ()
putItem env item = void $ send env (dPutItem item)

-- | Write item into the database only if it doesn't already exist.
insertItem  :: forall a r m. (DynamoTable a r, MonadResource m) => Env -> a -> m ()
insertItem env item = do
  let keyfields = primaryFields (Proxy :: Proxy a)
      -- Create condition attribute_not_exist(hash_key)
      pkeyMissing = (AttrMissing . nameGenPath . pure . IntraName) $ head keyfields
      (expr, attnames, attvals) = dumpCondition pkeyMissing
      cmd = dPutItem item & D.putItem_expressionAttributeNames ?~ attnames
                          & D.putItem_conditionExpression ?~ expr
                          & bool (D.putItem_expressionAttributeValues ?~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
  void $ send env cmd


-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table.
getItem :: forall m a r. (DynamoTable a r, MonadResource m, MonadThrow m) => Env -> Consistency -> Proxy a -> PrimaryKey a r -> m (Maybe a)
getItem env consistency p key = do
  let cmd = dGetItem p key & D.getItem_consistentRead . consistencyL .~ consistency
  rs <- send env cmd
  let result = fromMaybe mempty (rs ^. D.getItemResponse_item)
  if | null result -> return Nothing
     | otherwise ->
          case dGsDecode result of
              Right res -> return (Just res)
              Left err -> Control.Monad.Catch.throwM (DynamoException $ "Cannot decode item: " Data.Semigroup.<> err)

-- | Delete item from the database by specifying the primary key.
deleteItemByKey :: forall m a r. (DynamoTable a r, MonadResource m) => Env -> Proxy a -> PrimaryKey a r -> m ()
deleteItemByKey env p pkey = void $ send env (dDeleteItem p pkey)

-- | Delete item from the database by specifying the primary key and a condition.
-- Throws AWS exception if the condition does not succeed.
deleteItemCondByKey :: forall m a r.
    (DynamoTable a r, MonadResource m) => Env -> Proxy a -> PrimaryKey a r -> FilterCondition a -> m ()
deleteItemCondByKey env p pkey cond =
    let (expr, attnames, attvals) = dumpCondition cond
        cmd = dDeleteItem p pkey & D.deleteItem_expressionAttributeNames ?~ attnames
                                 & bool (D.deleteItem_expressionAttributeValues ?~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                 & D.deleteItem_conditionExpression ?~ expr
    in void (send env cmd)

-- | Generate update item object; automatically adds condition for existence of primary
-- key, so that only existing objects are modified
dUpdateItem :: forall a r. DynamoTable a r
          => Proxy a -> PrimaryKey a r -> Action a -> Maybe (FilterCondition a) ->  Maybe D.UpdateItem
dUpdateItem p pkey actions mcond =
    genAction <$> dumpActions actions
  where
    keyfields = primaryFields p
        -- Create condition attribute_exists(hash_key)
    pkeyExists = (AttrExists . nameGenPath . pure . IntraName) (head keyfields)

    genAction actparams =
        D.newUpdateItem (tableName p) & D.updateItem_key .~ dKeyToAttr p pkey
                                      & addActions actparams
                                      & addCondition (Just pkeyExists <> mcond)

    addActions (expr, attnames, attvals) =
          (D.updateItem_updateExpression ?~ expr)
            . (D.updateItem_expressionAttributeNames %~ Just . (<> attnames) . fromMaybe mempty)
            . bool (D.updateItem_expressionAttributeValues %~ Just . (<> attvals) . fromMaybe mempty) id (null attvals)
    addCondition (Just cond) =
        let (expr, attnames, attvals) = dumpCondition cond
        in  (D.updateItem_conditionExpression ?~ expr)
            . (D.updateItem_expressionAttributeNames %~ Just . (<> attnames) . fromMaybe mempty)
            . bool (D.updateItem_expressionAttributeValues %~ Just . (<> attvals) . fromMaybe mempty) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
    addCondition Nothing = id -- Cannot happen anyway


-- | Update item in a table.
--
-- > updateItem (Proxy :: Proxy Test) (12, "2") (colCount +=. 100)
updateItemByKey_ :: forall a m r.
      (DynamoTable a r, MonadResource m) => Env -> Proxy a -> PrimaryKey a r -> Action a -> m ()
updateItemByKey_ env p pkey actions
  | Just cmd <- dUpdateItem p pkey actions Nothing = void $ send env cmd
  | otherwise = return ()

-- | Update item in a database, return an updated version of the item.
updateItemByKey :: forall a m r.
      (DynamoTable a r, MonadResource m, MonadThrow m) => Env -> Proxy a -> PrimaryKey a r -> Action a -> m a
updateItemByKey env p pkey actions
  | Just cmd <- dUpdateItem p pkey actions Nothing = do
        rs <- send env (cmd & D.updateItem_returnValues ?~ D.ReturnValue_ALL_NEW)
        case dGsDecode (fromMaybe mempty (rs ^. D.updateItemResponse_attributes)) of
            Right res -> return res
            Left err -> throwM (DynamoException $ "Cannot decode item: " <> err)
  | otherwise = do
      rs <- getItem env Strongly p pkey
      case rs of
          Just res -> return res
          Nothing -> throwM (DynamoException "Cannot decode item.")

-- | Update item in a table while specifying a condition.
updateItemCond_ :: forall a m r. (DynamoTable a r, MonadResource m)
    => Env ->Proxy a -> PrimaryKey a r -> FilterCondition a -> Action a -> m ()
updateItemCond_ env p pkey cond actions
  | Just cmd <- dUpdateItem p pkey actions (Just cond) = void $ send env cmd
  | otherwise = return ()

-- | Delete a table from DynamoDB.
deleteTable :: (DynamoTable a r, MonadResource m) => Env -> Proxy a -> m ()
deleteTable env p = void $ send env (D.newDeleteTable (tableName p))

-- | Extract primary key from a record.
--
-- You can use this on both main table or on index tables if they contain the primary key from
-- the main table. Table key is always projected to indexes anyway, so just define it in
-- every index.
tableKey :: forall a parent key. ContainsTableKey a parent key => a -> key
tableKey = dTableKey

-- $intro
--
-- This library is operated in the following way:
--
-- * Create instances for your custom types using "Database.DynamoDB.Types"
-- * Create ordinary datatypes with records
-- * Use functions from "Database.DynamoDB.TH" to derive appropriate instances
-- * Optionally call generated migration function to automatically create
--   tables and indices
-- * Call functions from this module to access the database
--
-- The library does its best to ensure that only correct DynamoDB
-- operations are allowed. There are some limitations of DynamoDB
-- regarding access to empty values, but the library takes care
-- of this reasonably well.
--
-- Example of use
--
-- You may need to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment
-- variables.
--
-- @
-- data Test = Test {
--     category :: T.Text
--   , messageid :: T.Text
--   , subject :: T.Text
-- } deriving (Show)
-- mkTableDefs "migrate" (tableConfig "" (''Test, WithRange) [] [])
-- @
--
-- This code creates appropriate instances for the table and the columns. It creates
-- global variables `colCategory`, `colMessageid` and `colSubject` that can be used
-- in filtering conditions or update queries.
--
-- @
-- main = do
--    lgr <- newLogger Info stdout
--    env <- newEnv NorthVirginia Discover
--    -- Override, use DynamoDD on localhost
--    let dynamo = setEndpoint False "localhost" 8000 dynamoDB
--    let newenv = env & configure dynamo
--                     & set envLogger lgr
--    runResourceT $ runAWS newenv $ do
--        -- Create tables and indexes
--        migrate mempty Nothing
--        -- Save data to database
--        putItem (Test "news" "1-2-3-4" "New subject")
--        -- Fetch data given primary key
--        item <- getItem env Eventually tTest ("news", "1-2-3-4")
--        liftIO $ print item       -- (item :: Maybe Test)
--        -- Scan data using filter condition, return 10 results
--        items <- scanCond tTest (subject' ==. "New subejct") 10
--        print items         -- (items :: [Test])
-- @
--
-- See examples/ and test/ directories for more detail examples.

-- $proxy
--
-- In order to avoid ambiguity errors, most API calls need a 'Proxy' argument
-- to find out on which table or index to operate. These proxies are automatically
-- generated as a name of type prepended with "t" for tables and "i" for indexes.
--
-- A proxy for table Test will have name tTest, for index TestIndex the name will
-- be iTestIndex.

-- $lens
--
-- If the field names in the table record start with an underscore, the lens
-- get automatically generated for accessing the fields. The lens are polymorphic,
-- you can use them to access the fields of both main table and all the indexes.
--
-- @
-- data Test = Test {
--     _messageid :: T.Text
--   , _category :: T.Text
--   , _subject :: T.Text
-- } deriving (Show)
-- data TestIndex = TestIndex {
--     i_category :: T.Text
--   , i_messageid :: T.Text
-- }
-- mkTableDefs "migrate" (tableConfig "" (''Test, WithRange) [(''TestIndex, NoRange)] [])
--
-- doWithTest :: Test -> ...
-- doWithTest item = (item ^. category) ...
--
-- doWithItemIdx :: TestIndex -> ..
-- getCategoryIdx item = (item ^. category) ...
-- @

-- $conversion
--
-- Given a type 'Test' and an index type 'TestIndex',
-- a function 'toTest' is created that converts from 'TestIndex' to 'Test'.
-- Such function is created only if 'TestIndex' projects
-- all fields from 'Test'.
