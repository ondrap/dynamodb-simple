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
  , qConsistentRead, qExclusiveStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition
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

import           Control.Lens                        ((%~), (.~), (^.))
import           Control.Monad                       (void)
import           Control.Monad.Catch                 (throwM)
import           Data.Bool                           (bool)
import           Data.Function                       ((&))
import           Data.HashMap.Strict                 (HashMap)
import           Data.List.NonEmpty                  (NonEmpty(..))
import           Data.Monoid                         ((<>))
import           Data.Proxy
import qualified Data.Text                           as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.DeleteItem     as D
import qualified Network.AWS.DynamoDB.GetItem        as D
import qualified Network.AWS.DynamoDB.UpdateItem     as D

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types
import           Database.DynamoDB.Update
import           Database.DynamoDB.BatchRequest
import           Database.DynamoDB.QueryRequest


dDeleteItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
dDeleteItem p pkey = D.deleteItem (tableName p) & D.diKey .~ dKeyAndAttr p pkey

dGetItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.GetItem
dGetItem p pkey = D.getItem (tableName p) & D.giKey .~ dKeyAndAttr p pkey

dUpdateItem :: (DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.UpdateItem
dUpdateItem p pkey = D.updateItem (tableName p) & D.uiKey .~ dKeyAndAttr p pkey


-- | Write item into the database.
putItem :: (MonadAWS m, DynamoTable a r) => a -> m ()
putItem item = void $ send (dPutItem item)

-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table.
getItem :: forall m a r range hash rest.
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
