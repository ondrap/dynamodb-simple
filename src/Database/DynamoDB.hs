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

    -- * Data types
    DynamoException(..)
  , Consistency(..)
  , Column
    -- * Attribute path combinators
    , (<.>), (<!>), (<!:>)
    -- * Data query
  , getItem
  , getItemBatch
  , queryKey
  , queryKeyCond
  , scan
  , scanCond
    -- * Data modification
  , putItem
  , putItemBatch
  , updateItem
  , updateItemCond
  , deleteItem
  , deleteItemBatch
  , deleteItemCond
) where

import           Control.Lens                        (Iso', at, iso, ix,
                                                      toListOf, view, (%~),
                                                      (.~), (^.))
import           Control.Monad                       (void)
import           Control.Monad.Catch                 (throwM)
import           Control.Monad.Trans.Control         (MonadBaseControl)
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


dDeleteItem :: (ItemOper a r, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
dDeleteItem p pkey = D.deleteItem (tableName p) & D.diKey .~ dKeyAndAttr p pkey

dDeleteRequest :: (ItemOper a r, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteRequest
dDeleteRequest p pkey = D.deleteRequest & D.drKey .~ dKeyAndAttr p pkey

dGetItem :: (ItemOper a r, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.GetItem
dGetItem p pkey = D.getItem (tableName p) & D.giKey .~ dKeyAndAttr p pkey

dUpdateItem :: (ItemOper a r, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.UpdateItem
dUpdateItem p pkey = D.updateItem (tableName p) & D.uiKey .~ dKeyAndAttr p pkey


-- | Write item into the database.
putItem :: (MonadAWS m, DynamoTable a r) => a -> m ()
putItem item = void $ send (dPutItem item)

-- | Batch write into the database.
putItemBatch :: forall m a r. (MonadAWS m, DynamoTable a r) => NonEmpty a -> m ()
putItemBatch items =
  let tblname = tableName (Proxy :: Proxy a)
      wrequests = fmap mkrequest items
      mkrequest item = D.writeRequest & D.wrPutRequest .~ Just (D.putRequest & D.prItem .~ gdEncode item)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd

-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table.
getItem :: forall m a r range hash rest.
    (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest])
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
    (MonadAWS m, MonadBaseControl IO m, ItemOper a r, Code a ~ '[ hash ': range ': rest])
    => Consistency -> NonEmpty (PrimaryKey (Code a) r) -> m [a]
getItemBatch consistency keys = do
    let tblname = tableName (Proxy :: Proxy a)
        wkaas = fmap (dKeyAndAttr (Proxy :: Proxy a)) keys
        kaas = D.keysAndAttributes wkaas & D.kaaConsistentRead . consistencyL .~ consistency
        cmd = D.batchGetItem & D.bgiRequestItems . at tblname .~ Just kaas

    runConduit $ paginate cmd =$= rsDecode (toListOf (D.bgirsResponses . ix tblname . traverse))
                              =$= CL.consume

-- | Delete item from the database by specifying the primary key.
deleteItem :: forall m a r hash range rest.
    (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> m ()
deleteItem p pkey = void $ send (dDeleteItem p pkey)

-- | Batch version of 'deleteItem'.
deleteItemBatch :: forall m a r range hash rest.
    (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> NonEmpty (PrimaryKey (Code a) r) -> m ()
deleteItemBatch p keys =
  let tblname = tableName p
      wrequests = fmap mkrequest keys
      mkrequest key = D.writeRequest & D.wrDeleteRequest .~ Just (dDeleteRequest p key)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd

-- | Delete item from the database by specifying the primary key and a condition.
-- Throws AWS exception if the condition does not succeed.
deleteItemCond :: forall m a r hash range rest.
    (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> FilterCondition a -> m ()
deleteItemCond p pkey cond =
    let (expr, attnames, attvals) = dumpCondition cond
        cmd = dDeleteItem p pkey & D.diExpressionAttributeNames .~ attnames
                                 & bool (D.diExpressionAttributeValues .~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                 & D.diConditionExpression .~ Just expr
    in void (send cmd)

-- | Helper function to decode data from the conduit.
rsDecode :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t, All2 DynamoEncodable (Code a))
    => (i -> [HashMap T.Text D.AttributeValue]) -> Conduit i m a
rsDecode trans = CL.mapFoldable trans =$= CL.mapM decoder
  where
    decoder item =
      case gdDecode item of
        Just res -> return res
        Nothing -> throwM (DynamoException $ "Error decoding item: " <> T.pack (show item))

-- | Query item in a database using range key.
--   Throws 'DynamoException' if an item cannot be decoded.
queryKey :: forall a t m hash range rest. (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest])
  => Consistency -> hash -> Maybe (RangeOper range) -> Source m a
queryKey consistency key range = do
    let query = dQueryKey (Proxy :: Proxy a) key range & D.qConsistentRead . consistencyL .~ consistency
    paginate query =$= rsDecode (view D.qrsItems)

-- | Query item in a database, uses filter condition to further filter items server side
queryKeyCond :: forall a t m hash range rest. (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest])
  => Consistency -> hash -> Maybe (RangeOper range) -> FilterCondition a -> Source m a
queryKeyCond consistency key range cond = do
    let (expr, attnames, attvals) = dumpCondition cond
        query = dQueryKey (Proxy :: Proxy a) key range & D.qConsistentRead . consistencyL .~ consistency
                                                       & D.qExpressionAttributeNames %~ (<> attnames)
                                                       & bool (D.qExpressionAttributeValues %~ (<> attvals)) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                                       & D.qFilterExpression .~ Just expr
    paginate query =$= rsDecode (view D.qrsItems)

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
updateItem :: forall a m r hash range rest.
      (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest ])
    => Proxy a -> PrimaryKey (Code a) r -> [Action a] -> m ()
updateItem p pkey actions
  | Just (expr, attnames, attvals) <- dumpActions actions = do
        let cmd = dUpdateItem p pkey  & D.uiUpdateExpression .~ Just expr
                                      & D.uiExpressionAttributeNames %~ (<> attnames)
                                      & bool (D.uiExpressionAttributeValues %~ (<> attvals)) id (null attvals)
        void $ send cmd
  | otherwise = return ()

-- | Update item in a table while specifying a condition
updateItemCond :: forall a m r hash range rest.
      (MonadAWS m, ItemOper a r, Code a ~ '[ hash ': range ': rest ])
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


-- | Combine attributes from nested structures.
--
-- > colAddress <.> colStreet
(<.>) :: forall typ col1 typ2 col2.
        (InCollection col2 typ 'NestedPath, ColumnInfo col1, ColumnInfo col2)
      => Column typ 'TypColumn col1 -> Column typ2 'TypColumn col2 -> Column typ2 'TypColumn col1
(<.>) (Column (a1 :| rest1)) (Column (a2 :| rest2)) = Column (a1 :| rest1 ++ (a2 : rest2))
-- We need to associate from the right
infixl 7 <.>

-- | Access an index in a nested list.
--
-- > colUsers <!> 0 <.> colName
(<!>) :: forall typ col. ColumnInfo col => Column [typ] 'TypColumn col -> Int -> Column typ 'TypColumn col
(<!>) (Column (a1 :| rest)) num = Column (a1 :| (rest ++ [IntraIndex num]))
infixl 8 <!>

-- | Access a key in a nested hashmap.
--
-- > colPhones <!:> "mobile" <.> colNumber
(<!:>) :: forall typ col key. (ColumnInfo col, IsText key)
    => Column (HashMap key typ) 'TypColumn col -> T.Text -> Column typ 'TypColumn col
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
