{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiWayIf       #-}
module Database.DynamoDb.Migration (
  runMigration
) where

import           Control.Lens                       (ix, over, (%~), (.~), (^.),
                                                     _1)
import           Control.Monad                      (forM_, void)
import           Control.Monad.Catch                (throwM)
import           Data.Function                      ((&))
import           Data.List                          (nub)
import           Data.Proxy
import           Network.AWS
import qualified Network.AWS.DynamoDB.CreateTable   as D
import qualified Network.AWS.DynamoDB.DescribeTable as D
import qualified Network.AWS.DynamoDB.Types         as D

import           Database.DynamoDb.Class
import           Database.DynamoDb.Types

tryMigration :: MonadAWS m => D.CreateTable -> D.TableDescription -> m ()
tryMigration tabledef descr = do
  -- Check
  return ()

createOrMigrate :: MonadAWS m => D.CreateTable -> m ()
createOrMigrate tabledef = do
  let tblname = tabledef ^. D.ctTableName
  ers <- trying _ServiceError $ send (D.describeTable tblname)
  case ers of
    Left _ -> void $ send tabledef
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
