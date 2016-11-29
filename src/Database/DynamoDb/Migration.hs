{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiWayIf       #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Database.DynamoDb.Migration (
  runMigration
) where

import qualified Data.Text as T
import Control.Concurrent (threadDelay)
import           Control.Lens                       (ix, over, (%~), (.~), (^.),
                                                     _1, _Just, (^?))
import           Control.Monad                      (forM_, void)
import           Control.Monad.Catch                (throwM)
import           Data.Function                      ((&))
import           Data.List                          (nub)
import           Data.Proxy
import           Network.AWS
import qualified Network.AWS.DynamoDB.CreateTable   as D
import qualified Network.AWS.DynamoDB.DescribeTable as D
import qualified Network.AWS.DynamoDB.Types         as D
import Control.Monad.Loops (whileM_)
import Control.Monad.IO.Class (liftIO)

import           Database.DynamoDb.Class
import           Database.DynamoDb.Types

getTableStatus :: MonadAWS m => T.Text -> m D.TableStatus
getTableStatus tblname = do
  rs <- send (D.describeTable tblname)
  case rs ^? D.drsTable . _Just . D.tdTableStatus . _Just of
      Just status -> return status
      Nothing -> throwM (DynamoException "getTableStatus - did not get correct data")


-- | Periodically check state of table, until it is ACTIVE
waitUntilTableActive :: forall m. MonadAWS m => T.Text -> m ()
waitUntilTableActive name = whileM_ tableIsNotActive (liftIO $ threadDelay 5000000)
  where
    tableIsNotActive :: m Bool
    tableIsNotActive = (/= D.Active) <$> getTableStatus name


tryMigration :: MonadAWS m => D.CreateTable -> D.TableDescription -> m ()
tryMigration tabledef descr = do
  -- Check that attribute definitions do not conflict
  -- TODO
  -- Check key schema on the main table, fail if it changed
  -- TODO
  -- Check each index for
  -- -- Delete superfluous indices
  -- -- Check if index with same name has correct KeySchema;
  -- -- Check if index with same name has same projection
  -- -- * If any doesn't agree, drop index and recreate it
  -- Create any non-existent indexes
  return ()

createOrMigrate :: MonadAWS m => D.CreateTable -> m ()
createOrMigrate tabledef = do
  let tblname = tabledef ^. D.ctTableName
  ers <- trying _ServiceError $ send (D.describeTable tblname)
  case ers of
    Left _ -> void $ send tabledef -- table doesn't exist, create a new one
    Right rs
      | Just descr <- rs ^. D.drsTable -> tryMigration tabledef descr
      | otherwise -> throwM (DynamoException "Didn't receive correct table description.")

runMigration :: (DynamoTable table r IsTable, MonadAWS m) =>
  Proxy table -> [D.ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])] -> m ()
runMigration ptbl apindices = do
  let tbl = createTable ptbl (D.provisionedThroughput 5 5)
      indices = map ($ D.provisionedThroughput 5 5) apindices
      idattrs = concatMap snd indices
  let final = if | null apindices -> tbl
                 | otherwise -> tbl & D.ctGlobalSecondaryIndexes .~ map fst indices
                                    & D.ctAttributeDefinitions %~ (\old -> nub (concat (old : [idattrs])))
  createOrMigrate final
  return ()
