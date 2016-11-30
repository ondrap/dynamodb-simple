{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.DynamoDb.Migration (
  runMigration
) where

import           Control.Concurrent                 (threadDelay)
import           Control.Lens                       (ix, over, use, view, (%~),
                                                     (.~), (^.), (^..), (^?),
                                                     _1, _Just, filtered, set)
import           Control.Monad                      (forM_, unless, void, when)
import           Control.Monad.Catch                (throwM)
import           Control.Monad.IO.Class             (liftIO)
import           Control.Monad.Loops                (whileM_)
import           Control.Monad.Trans.AWS            (AWSConstraint)
import           Data.ByteString.Builder            (Builder, stringUtf8)
import           Data.Function                      ((&))
import qualified Data.HashMap.Strict                as HMap
import           Data.List                          (nub)
import           Data.Monoid                        ((<>))
import           Data.Proxy
import qualified Data.Text                          as T
import           Data.Text.Encoding                 (encodeUtf8Builder)
import           Network.AWS
import qualified Network.AWS.DynamoDB.CreateTable   as D
import qualified Network.AWS.DynamoDB.DescribeTable as D
import qualified Network.AWS.DynamoDB.UpdateTable as D
import qualified Network.AWS.DynamoDB.Types         as D
import Data.Maybe (mapMaybe)

import           Database.DynamoDb.Class
import           Database.DynamoDb.Types

getTableDescription :: MonadAWS m => T.Text -> m D.TableDescription
getTableDescription tblname = do
  rs <- send (D.describeTable tblname)
  case rs ^? D.drsTable . _Just of
      Just descr -> return descr
      Nothing -> throwM (DynamoException "getTableStatus - did not get correct data")

-- | Periodically check state of table, until it is ACTIVE
waitUntilTableActive :: forall r m. (AWSConstraint r m, MonadAWS m) => T.Text -> Bool -> m ()
waitUntilTableActive name checkindex =
    whileM_ tableIsNotActive $ do
        logmsg Info $ "Waiting for table " <> name <> " and its indices to become active"
        liftIO $ threadDelay 5000000
  where
    tableIsNotActive :: m Bool
    tableIsNotActive = do
        descr <- getTableDescription name
        status <- maybe (throwM (DynamoException "Missing table status")) return (descr ^. D.tdTableStatus)
        let idxstatus = descr ^.. D.tdGlobalSecondaryIndexes . traverse . D.gsidIndexStatus . _Just
        if | checkindex -> return (status /= D.Active || any (/= D.ISActive) idxstatus)
           | otherwise -> return (status /= D.Active)

-- | Delete specified indices from the database
deleteIndices :: forall m. MonadAWS m => T.Text -> [T.Text] -> m ()
deleteIndices tblname indices = do
  let idxupdates = map (\name -> set D.gsiuDelete (Just $ D.deleteGlobalSecondaryIndexAction name) D.globalSecondaryIndexUpdate) indices
      cmd = D.updateTable tblname & D.utGlobalSecondaryIndexUpdates .~ idxupdates
  void $ send cmd

-- | Update table with specified new indices
createIndices :: forall m. MonadAWS m => D.CreateTable -> [D.GlobalSecondaryIndex] -> m ()
createIndices table indices = do
    let tblname = table ^. D.ctTableName
    let idxupdates = map (\idx -> set D.gsiuCreate (Just $ mkidx idx) D.globalSecondaryIndexUpdate) indices
        cmd = D.updateTable tblname & D.utGlobalSecondaryIndexUpdates .~ idxupdates
                                    & D.utAttributeDefinitions .~ (table ^. D.ctAttributeDefinitions)
    -- TODO: add attribute schema
    void $ send cmd
  where
    mkidx :: D.GlobalSecondaryIndex -> D.CreateGlobalSecondaryIndexAction
    mkidx idx = D.createGlobalSecondaryIndexAction (idx ^. D.gsiIndexName) (idx ^. D.gsiKeySchema)
                                                   (idx ^. D.gsiProjection) (idx ^. D.gsiProvisionedThroughput)

tryMigration :: (AWSConstraint r m, MonadAWS m) => D.CreateTable -> D.TableDescription -> m ()
tryMigration tabledef descr = do
  let tblname = tabledef ^. D.ctTableName
  waitUntilTableActive tblname False
  -- Check that attribute definitions do not conflict; intersection of definitions must be the same
  let attrToTup = (,) <$> view D.adAttributeName <*> view D.adAttributeType
      attrdefs = HMap.fromList $ map attrToTup (tabledef ^. D.ctAttributeDefinitions)
      olddefs = HMap.fromList $ map attrToTup (descr ^. D.tdAttributeDefinitions)
      commonkeys = HMap.intersectionWith (,) attrdefs olddefs
      conflicts = HMap.filter (uncurry (/=)) commonkeys
  unless (null conflicts) $ do
      let msg = "Table or index " <> tblname <> " has conflicting hash/range keys: " <> T.pack (show $ HMap.toList conflicts)
      logmsg Error msg
      throwM (DynamoException msg)

  -- Check key schema on the main table, fail if it changed
  let tblkeys = tabledef ^. D.ctKeySchema
      oldkeys = descr ^. D.tdKeySchema
  when (Just tblkeys /= oldkeys) $ do
      let msg = "Table " <> tblname <> " hash/range key mismatch; new table: "
                  <> T.pack (show tblkeys) <> ", old table: " <> T.pack (show oldkeys)
      logmsg Error msg
      throwM (DynamoException msg)

  -- Delete obsolete indices
  let newidxlist = tabledef ^. D.ctGlobalSecondaryIndexes
      oldidxlist = descr ^. D.tdGlobalSecondaryIndexes
      newidxnames = newidxlist ^.. traverse . D.gsiIndexName
      oldidxnames = oldidxlist ^.. traverse . D.gsidIndexName . _Just
      -- There are many maybes in the lenses
      todelete = oldidxlist ^.. traverse . D.gsidIndexName . _Just . filtered (`notElem` newidxnames)
  unless (null todelete) $ do
      logmsg Info $ "Deleting indices: " <> T.intercalate "," todelete
      waitUntilTableActive tblname True
      deleteIndices tblname todelete

  -- Check each index for
  -- -- Check if index with same name has correct KeySchema;
  -- -- Check if index with same name has same projection
  -- -- * If any doesn't agree, drop index and recreate it
  -- Create any non-existent indexes
  let tocreate = filter (\idx -> idx ^. D.gsiIndexName `notElem` oldidxnames) newidxlist
  unless (null tocreate) $ do
      logmsg Info $ "Create new indices: " <> T.intercalate "," (tocreate ^.. traverse . D.gsiIndexName)
      createIndices tabledef tocreate
  logmsg Info $ "Table " <> tblname <> " schema check done."

logmsg :: AWSConstraint r m => LogLevel -> T.Text -> m ()
logmsg level text = do
  logger <- view envLogger
  liftIO $ logger level (encodeUtf8Builder text)

prettyTableInfo :: D.CreateTable -> T.Text
prettyTableInfo tblinfo =
    tblname <> "(" <> tkeys <> ")" <> indexinfo idxlist
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
            logmsg Info ("Table " <> tblname <> " alread exists, checking schema.")
            tryMigration tabledef descr
        | otherwise -> throwM (DynamoException "Didn't receive correct table description.")

runMigration :: (DynamoTable table r IsTable, MonadAWS m) =>
  Proxy table -> [D.ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])] -> m ()
runMigration ptbl apindices = do
  let tbl = createTable ptbl (D.provisionedThroughput 5 5)
      indices = map ($ D.provisionedThroughput 5 5) apindices
      idxattrs = concatMap snd indices
  -- | Bug in amazonka, we must not set the attribute if it is empty
  -- see https://github.com/brendanhay/amazonka/issues/332
  let final = if | null apindices -> tbl
                 | otherwise -> tbl & D.ctGlobalSecondaryIndexes .~ map fst indices
                                    & D.ctAttributeDefinitions %~ (\old -> nub (concat (old : [idxattrs])))
  liftAWS $ createOrMigrate final
