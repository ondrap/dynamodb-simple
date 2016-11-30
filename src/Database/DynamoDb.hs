{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.DynamoDb (
    DynamoException(..)
  , Consistency(..)
  , putItem
  , putItemBatch
  , getItem
  , deleteItem
  , deleteItemBatch
  , deleteItemCond
) where

import           Control.Lens                        (Iso', at, iso, (.~), (^.))
import           Control.Monad                       (void)
import           Control.Monad.Catch                 (throwM)
import           Data.Function                       ((&))
import           Data.List.NonEmpty
import           Data.Monoid                         ((<>))
import           Data.Proxy
import qualified Data.Text                           as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.BatchWriteItem as D
import qualified Network.AWS.DynamoDB.DeleteItem     as D
import qualified Network.AWS.DynamoDB.GetItem        as D
import qualified Network.AWS.DynamoDB.Types          as D

import           Database.DynamoDb.Class
import           Database.DynamoDb.Filter
import           Database.DynamoDb.Types

-- | Parameter for queries involving read consistency settings
data Consistency = Eventually | Strongly
  deriving (Show)

-- | Lens to help set consistency
consistencyL :: Iso' (Maybe Bool) Consistency
consistencyL = iso tocons fromcons
  where
    tocons (Just True) = Strongly
    tocons _ = Eventually
    fromcons Strongly = Just True
    fromcons Eventually = Just False

-- | Save item into the database
putItem :: (MonadAWS m, DynamoTable a r t) => a -> m ()
putItem item = void $ send (dPutItem item)

-- | Batch write into the database
putItemBatch :: forall m a r t. (MonadAWS m, DynamoTable a r t) => NonEmpty a -> m ()
putItemBatch items =
  let tblname = tableName (Proxy :: Proxy a)
      wrequests = fmap mkrequest items
      mkrequest item = D.writeRequest & D.wrPutRequest .~ Just (D.putRequest & D.prItem .~ gdEncode item)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd

-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table
getItem :: forall m a r t key hash rest.
    (MonadAWS m, DynamoTable a r t, Code a ~ '[ key ': hash ': rest])
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

-- | Delete item from the database by specifying the primary key
deleteItem :: forall m a r t key hash rest.
    (MonadAWS m, DynamoTable a r t, Code a ~ '[ key ': hash ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> m ()
deleteItem p pkey = void $ send (dDeleteItem p pkey)

-- | Batch version of deleteItem
deleteItemBatch :: forall m a r t key hash rest.
    (MonadAWS m, DynamoTable a r t, Code a ~ '[ key ': hash ': rest])
    => Proxy a -> NonEmpty (PrimaryKey (Code a) r) -> m ()
deleteItemBatch p keys =
  let tblname = tableName p
      wrequests = fmap mkrequest keys
      mkrequest key = D.writeRequest & D.wrDeleteRequest .~ Just (dDeleteRequest p key)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  in void $ send cmd

-- | Delete item from the database by specifying the primary key and a condition
-- Throws exception if the condition does not succeed
deleteItemCond :: forall m a r t key hash rest.
    (MonadAWS m, DynamoTable a r t, Code a ~ '[ key ': hash ': rest])
    => Proxy a -> PrimaryKey (Code a) r -> FilterCondition a -> m ()
deleteItemCond p pkey cond =
  let (expr, attnames, attvals) = dumpCondition cond
      cmd = dDeleteItem p pkey & D.diExpressionAttributeNames .~ attnames
                               & D.diExpressionAttributeValues .~ attvals
                               & D.diConditionExpression .~ Just expr
  in void (send cmd)

-- | Query item in a database using range key
