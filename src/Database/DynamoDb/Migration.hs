{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.DynamoDb.Migration (
  runMigration
) where

import           Control.Concurrent                 (threadDelay)
import           Control.Lens                       (ix, over, use, view, (%~),
                                                     (.~), (^.), (^?), _1, (^..),
                                                     _Just)
import           Control.Monad                      (forM_, void, when, unless)
import           Control.Monad.Catch                (throwM)
import           Control.Monad.IO.Class             (liftIO)
import           Control.Monad.Loops                (whileM_)
import           Control.Monad.Trans.AWS            (AWSConstraint)
import           Data.ByteString.Builder                (Builder, stringUtf8)
import           Data.Function                      ((&))
import           Data.List                          (nub)
import           Data.Proxy
import qualified Data.Text                          as T
import           Data.Text.Encoding                 (encodeUtf8Builder)
import           Network.AWS
import qualified Network.AWS.DynamoDB.CreateTable   as D
import qualified Network.AWS.DynamoDB.DescribeTable as D
import qualified Network.AWS.DynamoDB.Types         as D
import Data.Monoid ((<>))
import Data.Maybe (isNothing)
import qualified Data.HashMap.Strict as HMap

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


tryMigration :: (AWSConstraint r m, MonadAWS m) => D.CreateTable -> D.TableDescription -> m ()
tryMigration tabledef descr = do
  let tblname = tabledef ^. D.ctTableName
  waitUntilTableActive tblname
  -- Check that attribute definitions do not conflict; intersection of definitions must be the same
  let attrToTup = (,) <$> view D.adAttributeName <*> view D.adAttributeType
      attrdefs = HMap.fromList $ map attrToTup (tabledef ^. D.ctAttributeDefinitions)
      olddefs = HMap.fromList $ map attrToTup (descr ^. D.tdAttributeDefinitions)
      commonkeys = HMap.intersectionWith (,) attrdefs olddefs
      conflicts = HMap.filter (uncurry (/=)) commonkeys
  unless (null conflicts) $ do
      let msg = "Table or index " <> tblname <> " has conflicting hash/range keys: " <> T.pack (show $ HMap.toList conflicts)
      logmsg Error (encodeUtf8Builder msg)
      throwM (DynamoException msg)

  -- Check key schema on the main table, fail if it changed
  let tblkeys = tabledef ^. D.ctKeySchema
      oldkeys = descr ^. D.tdKeySchema
  when (Just tblkeys /= oldkeys) $ do
      let msg = "Table " <> tblname <> " hash/range key mismatch; new table: "
                  <> T.pack (show tblkeys) <> ", old table: " <> T.pack (show oldkeys)
      logmsg Error (encodeUtf8Builder msg)
      throwM (DynamoException msg)
  -- Check each index for
  -- -- Delete superfluous indices
  -- -- Check if index with same name has correct KeySchema;
  -- -- Check if index with same name has same projection
  -- -- * If any doesn't agree, drop index and recreate it
  -- Create any non-existent indexes
  logmsg Info $ "Table " <> encodeUtf8Builder tblname <> " schema check done."

logmsg :: AWSConstraint r m => LogLevel -> Builder -> m ()
logmsg level text = do
  logger <- view envLogger
  liftIO $ logger level text

prettyTableInfo :: D.CreateTable -> Builder
prettyTableInfo tblinfo =
    encodeUtf8Builder tblname <> "(" <> encodeUtf8Builder tkeys <> ")" <> encodeUtf8Builder (indexinfo idxlist)
  where
    tblname = tblinfo ^. D.ctTableName
    tkeys = T.intercalate "," $ tblinfo ^.. (D.ctKeySchema . traverse . D.kseAttributeName)
    idxlist = tblinfo ^. D.ctGlobalSecondaryIndexes
    indexinfo [] = ""
    indexinfo lst = " with indexes: " <> T.intercalate ", " (map printidx lst)
    printidx idx = idx ^. D.gsiIndexName <> "(" <> ikeys idx <> ")"
    ikeys idx = T.intercalate "," $ idx ^.. (D.gsiKeySchema . traverse . D.kseAttributeName)

createOrMigrate :: (AWSConstraint r m, MonadAWS m) => D.CreateTable -> m ()
createOrMigrate tabledef = do
    let tblname = tabledef ^. D.ctTableName
    ers <- trying _ServiceError $ send (D.describeTable tblname)
    case ers of
      Left _ -> do
          logmsg Info ("Creating table: " <> prettyTableInfo tabledef)
          void $ send tabledef -- table doesn't exist, create a new one
      Right rs
        | Just descr <- rs ^. D.drsTable -> do
            logmsg Info ("Table " <> encodeUtf8Builder tblname <> " alread exists, checking schema.")
            tryMigration tabledef descr
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
  liftAWS $ createOrMigrate final
  return ()
