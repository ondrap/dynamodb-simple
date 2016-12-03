{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE ViewPatterns        #-}
{-# LANGUAGE TemplateHaskell        #-}

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
    -- * Indexed query
  , querySimple
  , query
  , QueryOpts
  , queryOpts
  , qConsistentRead, qExclusiveStartKey, qLimit, qDirection, qFilterCondition, qHashKey, qRangeCondition  
    -- * Data Scan
  , scan
  , scanCond
    -- * Data entry
  , putItem
  , putItemBatch
    -- * Data modification
  , updateItemByKey
  , updateItemCond
    -- * Deleting data
  , deleteItem
  , deleteItemBatchByKey
  , deleteItemCond
  , deleteItemCondByKey
    -- * Utility functions
  , itemToKey
) where

import           Control.Lens                        (Iso', at, iso, ix,
                                                      toListOf, view, (%~),
                                                      (.~), (^.))
import           Control.Lens.TH                     (makeLenses)
import           Control.Monad                       (void)
import           Control.Monad.Catch                 (throwM)
import           Data.Bool                           (bool)
import           Data.Conduit                        (Conduit, Source,
                                                      runConduit, (=$=))
import qualified Data.Conduit.List                   as CL
import           Data.Function                       ((&))
import           Data.HashMap.Strict                 (HashMap)
import           Data.List.NonEmpty
import           Data.Monoid                         ((<>))
import           Data.Proxy
import qualified Data.Text                           as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.BatchGetItem   as D
import qualified Network.AWS.DynamoDB.BatchWriteItem as D
import qualified Network.AWS.DynamoDB.DeleteItem     as D
import qualified Network.AWS.DynamoDB.GetItem        as D
import qualified Network.AWS.DynamoDB.Query          as D
import qualified Network.AWS.DynamoDB.Scan           as D
import qualified Network.AWS.DynamoDB.Types          as D
import qualified Network.AWS.DynamoDB.UpdateItem     as D
import           Network.AWS.Pager                   (AWSPager (..))
import           Numeric.Natural                     (Natural)

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types
import           Database.DynamoDB.Update


-- | Parameter for queries involving read consistency settings.
data Consistency = Eventually | Strongly
  deriving (Show)

-- | Lens to help set consistency.
consistencyL :: Iso' (Maybe Bool) Consistency
consistencyL = iso tocons fromcons
  where
    tocons (Just True) = Strongly
    tocons _ = Eventually
    fromcons Strongly = Just True
    fromcons Eventually = Just False


dDeleteItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
dDeleteItem p pkey = D.deleteItem (tableName p) & D.diKey .~ dKeyAndAttr p pkey

dDeleteRequest :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteRequest
dDeleteRequest p pkey = D.deleteRequest & D.drKey .~ dKeyAndAttr p pkey

dGetItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.GetItem
dGetItem p pkey = D.getItem (tableName p) & D.giKey .~ dKeyAndAttr p pkey

dUpdateItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.UpdateItem
dUpdateItem p pkey = D.updateItem (tableName p) & D.uiKey .~ dKeyAndAttr p pkey


-- | Write item into the database.
putItem :: (MonadAWS m, DynamoTable a r) => a -> m ()
putItem item = void $ send (dPutItem item)

-- | Batch write into the database.
putItemBatch :: forall m a r. (MonadAWS m, DynamoTable a r) => [a] -> m ()
putItemBatch (nonEmpty -> Just items) =
  let tblname = tableName (Proxy :: Proxy a)
      wrequests = fmap mkrequest items
      mkrequest item = D.writeRequest & D.wrPutRequest .~ Just (D.putRequest & D.prItem .~ gdEncode item)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd
putItemBatch _ = return ()

-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table.
getItem :: forall m a r t range hash rest.
    (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': rest])
    => Consistency -> PrimaryKey (Code a) r -> m (Maybe a)
getItem consistency key = do
  let cmd = dGetItem (Proxy :: Proxy a) key & D.giConsistentRead . consistencyL .~ consistency
  rs <- send cmd
  let result = rs ^. D.girsItem
  if | null result -> return Nothing
     | otherwise ->
          case gdDecode result of
              Just res -> return (Just res)
              Nothing -> throwM (DynamoException $ "Cannot decode item: " <> T.pack (show result))

-- | Orphan instance; amaznonka-dynamodb currently doesn't provide it
instance AWSPager D.BatchGetItem where
  page rq rs
    | null (rs ^. D.bgirsUnprocessedKeys) = Nothing
    | otherwise = Just $ rq & D.bgiRequestItems .~ (rs ^. D.bgirsUnprocessedKeys)

-- | Get batch of items. Run the command using pager
-- (though amaznoka-dynamodb doesn't have such instance), but fetch the whole result;
-- it should easily get in the memory, as there is at most 100 items to be sent.
getItemBatch :: forall m a r range hash rest.
    (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': rest])
    => Consistency -> [PrimaryKey (Code a) r] -> m [a]
getItemBatch consistency (nonEmpty -> Just keys) = do
    let tblname = tableName (Proxy :: Proxy a)
        wkaas = fmap (dKeyAndAttr (Proxy :: Proxy a)) keys
        kaas = D.keysAndAttributes wkaas & D.kaaConsistentRead . consistencyL .~ consistency
        cmd = D.batchGetItem & D.bgiRequestItems . at tblname .~ Just kaas

    runConduit $ paginate cmd =$= rsDecode (toListOf (D.bgirsResponses . ix tblname . traverse))
                              =$= CL.consume
getItemBatch _ _ = return []

-- | Delete item by providing the item; primary key is extracted and
-- 'deleteItemByKey' is called.
deleteItem :: forall m a r hash range rest.
    (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ':  range ': rest ])
    => a -> m ()
deleteItem item = deleteItemByKey (Proxy :: Proxy a) (itemToKey item)

-- | Delete item from the database by specifying the primary key.
deleteItemByKey :: forall m a r hash range rest.
    (MonadAWS m, HasPrimaryKey a r 'IsTable, DynamoTable a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> m ()
deleteItemByKey p pkey = void $ send (dDeleteItem p pkey)

-- | Batch version of 'deleteItemByKey'.
deleteItemBatchByKey :: forall m a r range hash rest.
    (MonadAWS m, HasPrimaryKey a r 'IsTable, DynamoTable a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> [PrimaryKey (Code a) r] -> m ()
deleteItemBatchByKey p (nonEmpty -> Just keys) =
  let tblname = tableName p
      wrequests = fmap mkrequest keys
      mkrequest key = D.writeRequest & D.wrDeleteRequest .~ Just (dDeleteRequest p key)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd
deleteItemBatchByKey _ _ = return ()

-- | Delete item from the database by specifying the primary key and a condition.
-- Throws AWS exception if the condition does not succeed.
deleteItemCondByKey :: forall m a r hash range rest.
    (MonadAWS m, HasPrimaryKey a r 'IsTable, DynamoTable a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> FilterCondition a -> m ()
deleteItemCondByKey p pkey cond =
    let (expr, attnames, attvals) = dumpCondition cond
        cmd = dDeleteItem p pkey & D.diExpressionAttributeNames .~ attnames
                                 & bool (D.diExpressionAttributeValues .~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                 & D.diConditionExpression .~ Just expr
    in void (send cmd)

-- | Primary key is extracted from the item and 'deleteItemCondByKey' is called.
deleteItemCond :: forall m a r hash range rest.
    (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': rest])
    => a -> FilterCondition a -> m ()
deleteItemCond item cond = deleteItemCondByKey (Proxy :: Proxy a) (itemToKey item) cond

-- | Helper function to decode data from the conduit.
rsDecode :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => (i -> [HashMap T.Text D.AttributeValue]) -> Conduit i m a
rsDecode trans = CL.mapFoldable trans =$= CL.mapM decoder
  where
    decoder item =
      case gdDecode item of
        Just res -> return res
        Nothing -> throwM (DynamoException $ "Error decoding item: " <> T.pack (show item))

-- | Scan/query direction
data Direction = Forward | Backward
  deriving (Show, Eq)

-- | Options for a generic query
data QueryOpts a hash range = QueryOpts {
    _qHashKey :: hash
  , _qRangeCondition :: Maybe (RangeOper range)
  , _qFilterCondition :: Maybe (FilterCondition a)
  , _qConsistentRead :: Consistency
  , _qDirection :: Direction
  , _qLimit :: Maybe Natural
  , _qExclusiveStartKey :: Maybe (hash, range)
  -- ^ Key at which the evaluation starts. When paging, this should be set to qrsLastEvaluatedKey
  -- of the last operation and the first item should be dropped (???). The qrsLastEvaluatedKey is
  -- not currently available in this API.
}
makeLenses ''QueryOpts
-- | Default settings for query options
queryOpts :: hash -> QueryOpts a hash range
queryOpts key = QueryOpts key Nothing Nothing Eventually Forward Nothing Nothing

-- | Generic query function. You can query table or indexes that have
-- a range key defined. The filter condition cannot access the hash and range keys.
query :: forall a t m v1 v2 hash range rest.
    (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
     DynamoScalar v1 hash, DynamoScalar v2 range)
  => QueryOpts a hash range -> Source m a
query q = do
    let cmd = dQueryKey (Proxy :: Proxy a) (q ^. qHashKey) (q ^. qRangeCondition)
                  & D.qConsistentRead . consistencyL .~ (q ^. qConsistentRead)
                  & D.qScanIndexForward .~ Just (q ^. qDirection == Forward)
                  & D.qLimit .~ (q ^. qLimit)
                  & addStartKey (q ^. qExclusiveStartKey)
                  & addCondition (q ^. qFilterCondition)
    paginate cmd =$= rsDecode (view D.qrsItems)
  where
    addCondition Nothing = id
    addCondition (Just cond) =
      let (expr, attnames, attvals) = dumpCondition cond
      in (D.qExpressionAttributeNames %~ (<> attnames))
           -- HACK; https://github.com/brendanhay/amazonka/issues/332
         . bool (D.qExpressionAttributeValues %~ (<> attvals)) id (null attvals)
         . (D.qFilterExpression .~ Just expr)
    addStartKey Nothing = id
    addStartKey (Just (key, range)) =
        D.qExclusiveStartKey .~ dKeyAndAttr (Proxy :: Proxy a) (key, range)

-- | Query item in a database using range key.
--
-- Simple to use function to query limited amount of data from database.
--
-- Throws 'DynamoException' if an item cannot be decoded.
querySimple :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> Natural -- ^ Maximum number of items to fetch
  -> m [a]
querySimple key range limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qLimit .~ Just limit
  runConduit $ query opts =$= CL.consume

-- | Read full contents of a table or index.
--
-- > runConduit $ scan Eventually =$= CL.mapM_ (\(i :: Test) -> liftIO (print i))
scan :: forall a m hash range rest r t.
    (MonadAWS m, Code a ~ '[ hash ': range ': rest], TableScan a r t) => Consistency -> Source m a
scan consistency = do
    let cmd = dScan (Proxy :: Proxy a) & D.sConsistentRead .consistencyL .~ consistency
    paginate cmd =$= rsDecode (view D.srsItems)

-- | Scan table using a given filter condition.
--
-- > runConduit $ scanCond Eventually (colAddress <!:> "Home" <.> colCity ==. "London")
-- >          =$= CL.mapM_ (\(i :: Test) -> liftIO (print i))
scanCond :: forall a m hash range rest r t.
    (MonadAWS m, Code a ~ '[ hash ': range ': rest], TableScan a r t)
    => Consistency -> FilterCondition a -> Source m a
scanCond consistency cond = do
    let (expr, attnames, attvals) = dumpCondition cond
        cmd = dScan (Proxy :: Proxy a) & D.sConsistentRead . consistencyL .~ consistency
                                       & D.sExpressionAttributeNames .~ attnames
                                       & bool (D.sExpressionAttributeValues .~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                       & D.sFilterExpression .~ Just expr
    paginate cmd =$= rsDecode (view D.srsItems)

-- | Update item in a table
--
-- > updateItem (Proxy :: Proxy Test) (12, "2") [colCount +=. 100]
updateItemByKey :: forall a m r hash range rest.
      (MonadAWS m, HasPrimaryKey a r 'IsTable, DynamoTable a r, Code a ~ '[ hash ': range ': rest ])
    => Proxy a -> PrimaryKey (Code a) r -> [Action a] -> m ()
updateItemByKey p pkey actions
  | Just (expr, attnames, attvals) <- dumpActions actions = do
        let cmd = dUpdateItem p pkey  & D.uiUpdateExpression .~ Just expr
                                      & D.uiExpressionAttributeNames %~ (<> attnames)
                                      & bool (D.uiExpressionAttributeValues %~ (<> attvals)) id (null attvals)
        void $ send cmd
  | otherwise = return ()

-- | Update item in a table while specifying a condition
updateItemCond :: forall a m r hash range rest.
      (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': rest ])
    => Proxy a -> PrimaryKey (Code a) r -> [Action a] -> FilterCondition a -> m ()
updateItemCond p pkey actions cond
  | Just (expr, attnames, actAttvals) <- dumpActions actions = do
        let (cexpr, cattnames, condAttvals) = dumpCondition cond
            attvals = actAttvals  <> condAttvals
            cmd = dUpdateItem p pkey  & D.uiUpdateExpression .~ Just expr
                                      & D.uiConditionExpression .~ Just cexpr
                                      & D.uiExpressionAttributeNames %~ (<> (attnames <> cattnames))
                                      & bool (D.uiExpressionAttributeValues %~ (<> attvals)) id (null attvals)
        void $ send cmd
  | otherwise = return ()

-- | Allow skipping over maybe types when using <.>
type family UnMaybe a :: * where
  UnMaybe (Maybe a) = a
  UnMaybe a = a

-- | Combine attributes from nested structures.
--
-- > colAddress <.> colStreet
(<.>) :: (InCollection col2 (UnMaybe typ) 'NestedPath)
      => Column typ 'TypColumn col1 -> Column typ2 'TypColumn col2 -> Column typ2 'TypColumn col1
(<.>) (Column (a1 :| rest1)) (Column (a2 :| rest2)) = Column (a1 :| rest1 ++ (a2 : rest2))
-- We need to associate from the right
infixl 7 <.>

-- | Access an index in a nested list.
--
-- > colUsers <!> 0 <.> colName
(<!>) :: Column [typ] 'TypColumn col -> Int -> Column typ 'TypColumn col
(<!>) (Column (a1 :| rest)) num = Column (a1 :| (rest ++ [IntraIndex num]))
infixl 8 <!>

-- | Access a key in a nested hashmap.
--
-- > colPhones <!:> "mobile" <.> colNumber
(<!:>) :: IsText key => Column (HashMap key typ) 'TypColumn col -> T.Text -> Column typ 'TypColumn col
(<!:>) (Column (a1 :| rest)) key = Column (a1 :| (rest ++ [IntraName (toText key)]))
infixl 8 <!:>


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
-- import qualified GHC.Generics as GHC
-- data Test = Test {
--     category :: T.Text
--   , messageid :: T.Text
--   , subject :: T.Text
-- } deriving (Show, GHC.Generic)
-- $(mkTableDefs "migrate" (''Test, True) [])
--
-- main = do
--    lgr <- newLogger Info stdout
--    env <- newEnv NorthVirginia Discover
--    -- Override, use DynamoDD on localhost
--    let dynamo = setEndpoint False "localhost" 8000 dynamoDB
--    let newenv = env & configure dynamo
--                     & set envLogger lgr
--    runResourceT $ runAWS newenv $ do
--        migrate (provisionedThroughput 5 5) [] -- Create tables, indexes
--        putItem (Test "news" "1-2-3-4" "New subject")
--        item <- getItem Eventually ("news", "1-2-3-4")
--        liftIO $ print (item :: Maybe Test)
-- @
