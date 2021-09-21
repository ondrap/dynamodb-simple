{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE ViewPatterns        #-}

module Database.DynamoDB.BatchRequest (
    putItemBatch
  , getItemBatch
  , deleteItemBatchByKey
) where

import           Control.Concurrent                  (threadDelay)
import           Control.Lens                        (at, ix, (.~), (?~), (^.), (^..))
import           Control.Monad                       (unless)
import           Control.Monad.Catch                 (throwM)
import           Control.Monad.IO.Class              (liftIO)
import           Control.Monad.Trans.Resource
import           Data.Function                       ((&))
import           Data.HashMap.Strict                 (HashMap)
import qualified Data.HashMap.Strict                 as HMap
import           Data.List.NonEmpty                  (NonEmpty(..))
import           Data.Maybe                          (fromMaybe)
import           Data.Monoid                         ((<>))
import           Data.Proxy
import qualified Data.Text                           as T
import           Network.AWS
import qualified Network.AWS.DynamoDB.BatchGetItem   as D
import qualified Network.AWS.DynamoDB.BatchWriteItem as D
import qualified Network.AWS.DynamoDB.Types          as D

import           Database.DynamoDB.Class
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types



-- | Retry batch operation, until unprocessedItems is empty.
--
-- TODO: we should use exponential backoff; currently we use a simple 1-sec threadDelay
retryWriteBatch :: MonadResource m => Env -> D.BatchWriteItem -> m ()
retryWriteBatch env cmd = do
  rs <- send env cmd
  let unprocessed = fromMaybe mempty (rs ^. D.batchWriteItemResponse_unprocessedItems)
  unless (null unprocessed) $ do
      liftIO $ threadDelay 1000000
      retryWriteBatch env (cmd & D.batchWriteItem_requestItems .~ unprocessed)

-- | Retry batch operation, until unprocessedItems is empty.
--
-- TODO: we should use exponential backoff; currently we use a simple 1-sec threadDelay
retryReadBatch :: MonadResource m => Env -> D.BatchGetItem -> m (HashMap T.Text [HashMap T.Text D.AttributeValue])
retryReadBatch env = go mempty
  where
    go previous cmd = do
      rs <- send env cmd
      let unprocessed = fromMaybe mempty (rs ^. D.batchGetItemResponse_unprocessedKeys)
          result = HMap.unionWith (++) previous (fromMaybe mempty (rs ^. D.batchGetItemResponse_responses))
      if | null unprocessed -> return result
         | otherwise -> do
              liftIO $ threadDelay 1000000
              go result (cmd & D.batchGetItem_requestItems .~ unprocessed)

-- | Chunk list according to batch operation limit
chunkBatch :: Int -> [a] -> [NonEmpty a]
chunkBatch limit (splitAt limit -> (x:xs, rest)) = (x :| xs) : chunkBatch limit rest
chunkBatch _ _ = []

-- | Batch write into the database.
--
-- The batch is divided to 25-item chunks, each is sent and retried separately.
-- If a batch fails on dynamodb exception, it is raised.
--
-- Note: On exception, the information about which items were saved is unavailable
putItemBatch :: forall m a r. (MonadResource m, DynamoTable a r) => Env -> [a] -> m ()
putItemBatch env lst = mapM_ go (chunkBatch 25 lst)
  where
    go items = do
      let tblname = tableName (Proxy :: Proxy a)
          wrequests = fmap mkrequest items
          mkrequest item = D.newWriteRequest & D.writeRequest_putRequest ?~ (D.newPutRequest & D.putRequest_item .~ gsEncode item)
          cmd = D.newBatchWriteItem & D.batchWriteItem_requestItems . at tblname ?~ wrequests
      retryWriteBatch env cmd


-- | Get batch of items.
getItemBatch :: forall m a r. (MonadResource m, MonadThrow m, DynamoTable a r) => Env -> Consistency -> [PrimaryKey a r] -> m [a]
getItemBatch env consistency lst = concat <$> mapM go (chunkBatch 100 lst)
  where
    go keys = do
        let tblname = tableName (Proxy :: Proxy a)
            wkaas = fmap (dKeyToAttr (Proxy :: Proxy a)) keys
            kaas = D.newKeysAndAttributes wkaas & D.keysAndAttributes_consistentRead . consistencyL .~ consistency
            cmd = D.newBatchGetItem & D.batchGetItem_requestItems . at tblname ?~ kaas

        tbls <- retryReadBatch env cmd
        mapM decoder (tbls ^.. ix tblname . traverse)
    decoder item =
        case dGsDecode item of
          Right res -> return res
          Left err -> Control.Monad.Catch.throwM (DynamoException $ "Error decoding item: " Data.Monoid.<> err )

dDeleteRequest :: DynamoTable a r => Proxy a -> PrimaryKey a r -> D.DeleteRequest
dDeleteRequest p pkey = D.newDeleteRequest & D.deleteRequest_key .~ dKeyToAttr p pkey

-- | Batch version of 'deleteItemByKey'.
--
-- Note: Because the requests are chunked, the information about which items
-- were deleted in case of exception is unavailable.
deleteItemBatchByKey :: forall m a r. (MonadResource m, DynamoTable a r) => Env -> Proxy a -> [PrimaryKey a r] -> m ()
deleteItemBatchByKey env p lst = mapM_ go (chunkBatch 25 lst)
  where
    go keys = do
      let tblname = tableName p
          wrequests = fmap mkrequest keys
          mkrequest key = D.newWriteRequest & D.writeRequest_deleteRequest ?~ dDeleteRequest p key
          cmd = D.newBatchWriteItem & D.batchWriteItem_requestItems . at tblname ?~ wrequests
      retryWriteBatch env cmd
