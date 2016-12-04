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
import           Control.Lens                        (at, ix, (.~), (^.), (^..))
import           Control.Monad                       (unless)
import           Control.Monad.Catch                 (throwM)
import           Control.Monad.IO.Class              (liftIO)
import           Data.Function                       ((&))
import           Data.HashMap.Strict                 (HashMap)
import qualified Data.HashMap.Strict                 as HMap
import           Data.List.NonEmpty                  (nonEmpty)
import           Data.Monoid                         ((<>))
import           Data.Proxy
import qualified Data.Text                           as T
import           Generics.SOP
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
retryWriteBatch :: MonadAWS m => D.BatchWriteItem -> m ()
retryWriteBatch cmd = do
  rs <- send cmd
  let unprocessed = rs ^. D.bwirsUnprocessedItems
  unless (null unprocessed) $ do
      liftIO $ threadDelay 1000000
      retryWriteBatch (cmd & D.bwiRequestItems .~ unprocessed)

-- | Retry batch operation, until unprocessedItems is empty.
--
-- TODO: we should use exponential backoff; currently we use a simple 1-sec threadDelay
retryReadBatch :: MonadAWS m => D.BatchGetItem -> m (HashMap T.Text [HashMap T.Text D.AttributeValue])
retryReadBatch = go mempty
  where
    go previous cmd = do
      rs <- send cmd
      let unprocessed = rs ^. D.bgirsUnprocessedKeys
          result = HMap.unionWith (++) previous (rs ^. D.bgirsResponses)
      if | null unprocessed -> return result
         | otherwise -> do
              liftIO $ threadDelay 1000000
              go result (cmd & D.bgiRequestItems .~ unprocessed)

-- | Chunk list according to batch operation limit
chunkBatch :: Int -> [a] -> ([a], [a])
chunkBatch limit = (,) <$> take limit <*> drop limit

-- | Batch write into the database.
--
-- The batch is divided to 25-item chunks, each is sent and retried separately.
-- If a batch fails on dynamodb exception, it is raised.
--
-- Note: On exception, the information about which items were saved is unavailable
putItemBatch :: forall m a r. (MonadAWS m, DynamoTable a r) => [a] -> m ()
putItemBatch (chunkBatch 25 -> (nonEmpty -> Just items, rest)) = do
    let tblname = tableName (Proxy :: Proxy a)
        wrequests = fmap mkrequest items
        mkrequest item = D.writeRequest & D.wrPutRequest .~ Just (D.putRequest & D.prItem .~ gdEncode item)
        cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
    retryWriteBatch cmd
    putItemBatch rest
putItemBatch _ = return ()


-- | Get batch of items. 
getItemBatch :: forall m a r range hash rest.
    (MonadAWS m, DynamoTable a r, HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': rest])
    => Consistency -> [PrimaryKey (Code a) r] -> m [a]
getItemBatch consistency = go []
  where
    go previous (chunkBatch 100 -> (nonEmpty -> Just keys, rest)) = do
        let tblname = tableName (Proxy :: Proxy a)
            wkaas = fmap (dKeyAndAttr (Proxy :: Proxy a)) keys
            kaas = D.keysAndAttributes wkaas & D.kaaConsistentRead . consistencyL .~ consistency
            cmd = D.batchGetItem & D.bgiRequestItems . at tblname .~ Just kaas

        tbls <- retryReadBatch cmd
        result <- mapM decoder (tbls ^.. ix tblname . traverse)
        go (result: previous) rest
    go previous _ = return (concat previous)
    decoder item =
        case gdDecode item of
          Just res -> return res
          Nothing -> throwM (DynamoException $ "Error decoding item: " <> T.pack (show item))

dDeleteRequest :: (HasPrimaryKey a r 'IsTable, Code a ~ '[ hash ': range ': xss ])
          => Proxy a -> PrimaryKey (Code a) r -> D.DeleteRequest
dDeleteRequest p pkey = D.deleteRequest & D.drKey .~ dKeyAndAttr p pkey

-- | Batch version of 'deleteItemByKey'.
--
-- Note: Because the requests are chunked, the information about which items
-- were deleted in case of exception is unavailable.
deleteItemBatchByKey :: forall m a r range hash rest.
    (MonadAWS m, HasPrimaryKey a r 'IsTable, DynamoTable a r, Code a ~ '[ hash ': range ': rest])
    => Proxy a -> [PrimaryKey (Code a) r] -> m ()
deleteItemBatchByKey p (chunkBatch 25 -> (nonEmpty -> Just keys, rest)) = do
  let tblname = tableName p
      wrequests = fmap mkrequest keys
      mkrequest key = D.writeRequest & D.wrDeleteRequest .~ Just (dDeleteRequest p key)
      cmd = D.batchWriteItem & D.bwiRequestItems . at tblname .~ Just wrequests
  retryWriteBatch cmd
  deleteItemBatchByKey p rest
deleteItemBatchByKey _ _ = return ()
