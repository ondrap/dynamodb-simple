{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.DynamoDB.Migration (
  runMigration
) where

import           Control.Arrow                      (first)
import           Control.Concurrent                 (threadDelay)
import           Control.Lens                       (set, view, (%~), (.~),
                                                     (^.), (^..), (^?), (?~), _1,
                                                     _Just)
import           Control.Monad                      (unless, void, when)
import           Control.Monad.Catch                (throwM, MonadCatch)
import           Control.Monad.IO.Class             (liftIO, MonadIO)
import           Control.Monad.Loops                (whileM_)
import           Control.Monad.Trans.Resource
import           Data.Bool                          (bool)
import           Data.Foldable                      (toList)
import           Data.Function                      ((&))
import qualified Data.HashMap.Strict                as HMap
import           Data.List                          (nub, (\\))
import           Data.Maybe                         (fromMaybe, mapMaybe)
import           Data.Monoid                        ((<>))
import           Data.Proxy
import qualified Data.Set                           as Set
import qualified Data.Text                          as T
import           Data.Text.Encoding                 (encodeUtf8Builder)
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.CreateTable   as D
import qualified Network.AWS.DynamoDB.DescribeTable as D
import qualified Network.AWS.DynamoDB.Types         as D
import qualified Network.AWS.DynamoDB.UpdateTable   as D

import           Database.DynamoDB.Class
import           Database.DynamoDB.Types

getTableDescription :: (MonadResource m, MonadThrow m) => Env -> T.Text -> m D.TableDescription
getTableDescription env tblname = do
  rs <- send env (D.newDescribeTable tblname)
  case rs ^? D.describeTableResponse_table . _Just of
      Just descr -> return descr
      Nothing -> throwM (DynamoException "getTableStatus - did not get correct data")

-- | Periodically check state of table, until it is ACTIVE
waitUntilTableActive :: forall r m. (MonadResource m, MonadThrow m) => Env -> T.Text -> Bool -> m ()
waitUntilTableActive env name checkindex =
    whileM_ tableIsNotActive $ do
        logmsg env Info $ "Waiting for table " <> name <> " and its indices to become active"
        liftIO $ threadDelay 5000000
  where
    tableIsNotActive :: m Bool
    tableIsNotActive = do
        descr <- getTableDescription env name
        status <- maybe (throwM (DynamoException "Missing table status")) return (descr ^. D.tableDescription_tableStatus)
        let idxstatus = descr ^.. D.tableDescription_globalSecondaryIndexes . _Just  . traverse . D.globalSecondaryIndexDescription_indexStatus . _Just
        if | checkindex -> return (status /= D.TableStatus_ACTIVE || any (/= D.IndexStatus_ACTIVE) idxstatus)
           | otherwise -> return (status /= D.TableStatus_ACTIVE)

-- | Delete specified indices from the database
deleteIndices :: forall m. MonadResource m => Env -> T.Text -> [T.Text] -> m ()
deleteIndices env tblname indices = do
  let idxupdates = map (\name -> set D.globalSecondaryIndexUpdate_delete (Just $ D.newDeleteGlobalSecondaryIndexAction name) D.newGlobalSecondaryIndexUpdate) indices
      cmd = D.newUpdateTable tblname & D.updateTable_globalSecondaryIndexUpdates ?~ idxupdates
  void $ send env cmd

-- | Update table with specified new indices
createIndices :: forall m. MonadResource m => Env -> D.CreateTable -> [D.GlobalSecondaryIndex] -> m ()
createIndices env table indices = do
    let tblname = table ^. D.createTable_tableName
        idxupdates = map (\idx -> set D.globalSecondaryIndexUpdate_create (Just $ mkidx idx) D.newGlobalSecondaryIndexUpdate) indices
        cmd = D.newUpdateTable tblname & D.updateTable_globalSecondaryIndexUpdates ?~ idxupdates
                                       & D.updateTable_attributeDefinitions ?~ (table ^. D.createTable_attributeDefinitions)
    void $ send env cmd
  where
    mkidx :: D.GlobalSecondaryIndex -> D.CreateGlobalSecondaryIndexAction
    mkidx idx = D.newCreateGlobalSecondaryIndexAction
      (idx ^. D.globalSecondaryIndex_indexName)
      (idx ^. D.globalSecondaryIndex_keySchema)
      (idx ^. D.globalSecondaryIndex_projection)
      & D.createGlobalSecondaryIndexAction_provisionedThroughput .~ (idx ^. D.globalSecondaryIndex_provisionedThroughput)

-- | Compare intersection of new and old indexes and find inconsistent ones
findInconsistentIdxes :: [D.GlobalSecondaryIndex] -> [D.GlobalSecondaryIndexDescription] -> [D.GlobalSecondaryIndex]
findInconsistentIdxes newidxes oldidxes =
    map fst $ filter hasConflict $ toList $ HMap.intersectionWith (,) newmap oldmap
  where
    newmap = HMap.fromList $ map (\idx -> (idx ^. D.globalSecondaryIndex_indexName, idx)) newidxes
    oldmap = HMap.fromList $ mapMaybe (\idx -> (,idx) <$> idx ^. D.globalSecondaryIndexDescription_indexName) oldidxes
    --
    hasConflict (newidx, oldix) = not (projectionOk newidx oldix && keysOk newidx oldix)
    keysOk newidx oldidx = Just (newidx ^. D.globalSecondaryIndex_keySchema) == oldidx ^. D.globalSecondaryIndexDescription_keySchema
    -- Assume the indices were created by this module and we want them exactly the same
    projectionOk newidx oldidx =
       -- Allow the index to have more fields than we know about
        newprojtype == Just D.ProjectionType_KEYS_ONLY || (newprojtype == oldprojtype && newkeys `Set.isSubsetOf` oldkeys)
      where
          newprojtype = newidx ^? D.globalSecondaryIndex_projection . D.projection_projectionType . _Just
          oldprojtype = oldidx ^? D.globalSecondaryIndexDescription_projection . _Just . D.projection_projectionType . _Just
          newkeys = Set.fromList $ newidx ^.. D.globalSecondaryIndex_projection . D.projection_nonKeyAttributes . _Just . traverse
          oldkeys = Set.fromList $ oldidx ^.. D.globalSecondaryIndexDescription_projection . _Just . D.projection_nonKeyAttributes . _Just . traverse

-- | Verbatim copy of findInconsistentIdxes, but changed to localSecondaryIndex structure
findInconsistentLocIdxes :: [D.LocalSecondaryIndex] -> [D.LocalSecondaryIndexDescription] -> [T.Text]
findInconsistentLocIdxes newidxes oldidxes =
    map (view (_1 . D.localSecondaryIndex_indexName)) $ filter hasConflict $ toList $ HMap.intersectionWith (,) newmap oldmap
  where
    newmap = HMap.fromList $ map (\idx -> (idx ^. D.localSecondaryIndex_indexName, idx)) newidxes
    oldmap = HMap.fromList $ mapMaybe (\idx -> (,idx) <$> idx ^. D.localSecondaryIndexDescription_indexName) oldidxes
    --
    hasConflict (newidx, oldix) = not (projectionOk newidx oldix && keysOk newidx oldix)
    keysOk newidx oldidx = Just (newidx ^. D.localSecondaryIndex_keySchema) == oldidx ^. D.localSecondaryIndexDescription_keySchema
    -- Assume the indices were created by this module and we want them exactly the same
    projectionOk newidx oldidx =
        newprojtype == Just D.ProjectionType_KEYS_ONLY || (newprojtype == oldprojtype && newkeys `Set.isSubsetOf` oldkeys)
      where
          newprojtype = newidx ^? D.localSecondaryIndex_projection . D.projection_projectionType . _Just
          oldprojtype = oldidx ^? D.localSecondaryIndexDescription_projection . _Just . D.projection_projectionType . _Just
          newkeys = Set.fromList $ newidx ^.. D.localSecondaryIndex_projection . D.projection_nonKeyAttributes . _Just . traverse
          oldkeys = Set.fromList $ oldidx ^.. D.localSecondaryIndexDescription_projection . _Just . D.projection_nonKeyAttributes . _Just . traverse


-- | Compare indexes and return list of indices to delete and to create; indices to recreate are included
compareIndexes :: D.CreateTable -> D.TableDescription -> ([T.Text], [D.GlobalSecondaryIndex])
compareIndexes tabledef descr = (todelete, tocreate)
  where
    newidxlist = fromMaybe mempty (tabledef ^. D.createTable_globalSecondaryIndexes)
    oldidxlist = fromMaybe mempty (descr ^. D.tableDescription_globalSecondaryIndexes)
    newidxnames = newidxlist ^.. traverse . D.globalSecondaryIndex_indexName
    oldidxnames = oldidxlist ^.. traverse . D.globalSecondaryIndexDescription_indexName . _Just
    --
    recreate = findInconsistentIdxes newidxlist oldidxlist
    todelete = map (view D.globalSecondaryIndex_indexName) recreate ++ (oldidxnames \\ newidxnames)
    tocreate = recreate ++ filter (\idx -> idx ^. D.globalSecondaryIndex_indexName `notElem` oldidxnames) newidxlist

compareLocalIndexes :: (MonadResource m, MonadThrow m) => D.CreateTable -> D.TableDescription -> m ()
compareLocalIndexes tabledef descr = do
  let newidxlist = fromMaybe mempty (tabledef ^. D.createTable_localSecondaryIndexes)
      oldidxlist = fromMaybe mempty (descr ^. D.tableDescription_localSecondaryIndexes)
      newidxnames = newidxlist ^.. traverse . D.localSecondaryIndex_indexName
      oldidxnames = oldidxlist ^.. traverse . D.localSecondaryIndexDescription_indexName . _Just
      missing = filter (`notElem` oldidxnames) newidxnames
  unless (null missing) $
      throwM (DynamoException ("Missing local secondary indexes: " <> T.intercalate "," missing))
  let inconsistent = findInconsistentLocIdxes newidxlist oldidxlist
  unless (null inconsistent) $
      throwM (DynamoException ("Inconsistent local index settings (projection/types): "
                                <> T.intercalate "," inconsistent))

-- | Change streaming settings on a table
changeStream :: (MonadResource m, MonadThrow m)
    => Env -> T.Text -> Maybe D.StreamSpecification -> Maybe D.StreamSpecification -> m ()
changeStream _ _ Nothing Nothing = return ()
changeStream env tblname (Just _) Nothing = do
    logmsg env Info "Disabling streaming."
    waitUntilTableActive env tblname False
    let enabled = False
        strspec = D.newStreamSpecification enabled
        cmd = D.newUpdateTable tblname & D.updateTable_streamSpecification ?~ strspec
    void (send env cmd)
changeStream env tblname Nothing (Just new) = do
    waitUntilTableActive env tblname False
    logmsg env Info "Enabling streaming."
    let cmd = D.newUpdateTable tblname & D.updateTable_streamSpecification ?~ new
    void (send env cmd)
changeStream env tblname (Just old) (Just new) = do
    changeStream env tblname (Just old) Nothing
    changeStream env tblname Nothing (Just new)


-- | Main table migration code
tryMigration :: (MonadResource m, MonadThrow m) => Env -> D.CreateTable -> D.TableDescription -> m ()
tryMigration env tabledef descr = do
    -- Check key schema on the main table, fail if it changed.
    let tblkeys = tabledef ^. D.createTable_keySchema
        oldtblkeys = descr ^. D.tableDescription_keySchema
    when (Just tblkeys /= oldtblkeys) $ do
        let msg = "Table " <> tblname <> " hash/range key mismatch; new table: "
                    <> T.pack (show tblkeys) <> ", old table: " <> T.pack (show oldtblkeys)
        logmsg env Error msg
        throwM (DynamoException msg)

    -- Check that types of key attributes are the same
    unless (null conflictTableAttrs) $ do
        let msg = "Table or index " <> tblname <> " has conflicting attribute key types: " <> T.pack (show conflictTableAttrs)
        logmsg env Error msg
        throwM (DynamoException msg)

    -- Check that local indexes are in sync with the settings. Fails if inconsistent.
    compareLocalIndexes tabledef descr

    -- Adjust indexes
    let (todelete, tocreate) = compareIndexes tabledef descr
    unless (null todelete) $ do
        waitUntilTableActive env tblname False
        logmsg env Info $ "Deleting indices: " <> T.intercalate "," todelete
        deleteIndices env tblname todelete
    unless (null tocreate) $ do
        waitUntilTableActive env tblname True
        logmsg env Info $ "Create new indices: " <> T.intercalate "," (tocreate ^.. traverse . D.globalSecondaryIndex_indexName)
        createIndices env tabledef tocreate
    -- Check streaming settings
    when (tabledef ^. D.createTable_streamSpecification /= descr ^. D.tableDescription_streamSpecification) $
        changeStream env tblname (descr ^. D.tableDescription_streamSpecification) (tabledef ^. D.createTable_streamSpecification)
    -- Done
    logmsg env Info $ "Table " <> tblname <> " schema check done."
  where
    tblname = tabledef ^. D.createTable_tableName
    -- Compare tableattribute types from old and new tables
    conflictTableAttrs =
        let attrToTup = (,) <$> view D.attributeDefinition_attributeName <*> view D.attributeDefinition_attributeType
            attrdefs = HMap.fromList $ map attrToTup (tabledef ^. D.createTable_attributeDefinitions)
            olddefs = HMap.fromList $ map attrToTup (fromMaybe mempty (descr ^. D.tableDescription_attributeDefinitions))
            commonkeys = HMap.intersectionWith (,) attrdefs olddefs
        in
            HMap.toList $ HMap.filter (uncurry (/=)) commonkeys


logmsg :: (MonadIO m) => Env -> LogLevel -> T.Text -> m ()
logmsg env level text = do
  let logger = envLogger env
  liftIO $ logger level (encodeUtf8Builder text)

prettyTableInfo :: D.CreateTable -> T.Text
prettyTableInfo tblinfo =
    tblname <> "(" <> tkeys <> ")" <> indexinfo idxlist
  where
    tblname = tblinfo ^. D.createTable_tableName
    tkeys = T.intercalate "," $ tblinfo ^.. (D.createTable_keySchema . traverse . D.keySchemaElement_attributeName)
    idxlist = fromMaybe mempty (tblinfo ^. D.createTable_globalSecondaryIndexes)
    indexinfo []  = ""
    indexinfo lst = " with indexes: " <> T.intercalate ", " (map printidx lst)
    printidx idx = idx ^. D.globalSecondaryIndex_indexName <> "(" <> ikeys idx <> ")"
    ikeys idx = T.intercalate "," $ idx ^.. (D.globalSecondaryIndex_keySchema . traverse . D.keySchemaElement_attributeName)

createOrMigrate :: (MonadResource m, MonadThrow m, MonadCatch m) => Env -> D.CreateTable -> m ()
createOrMigrate env tabledef = do
    let tblname = tabledef ^. D.createTable_tableName
    ers <- trying _ServiceError $ send env (D.newDescribeTable tblname)
    case ers of
      Left _ -> do
          logmsg env Info ("Creating table: " <> prettyTableInfo tabledef)
          void $ send env tabledef -- table doesn't exist, create a new one
          waitUntilTableActive env tblname True
      Right rs
        | Just descr <- rs ^. D.describeTableResponse_table -> do
            logmsg env Info ("Table " <> tblname <> " alread exists, checking schema.")
            tryMigration env tabledef descr
        | otherwise -> throwM (DynamoException "Didn't receive correct table description.")

runMigration :: (TableCreate table r, Code table ~ '[ hash ': range ': rest ], MonadResource m, MonadThrow m, MonadCatch m)
  => Env
  -> Proxy table
  -> [D.ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])]
  -> [(D.LocalSecondaryIndex, [D.AttributeDefinition])]
  -> HMap.HashMap T.Text D.ProvisionedThroughput
  -> Maybe D.StreamViewType
  -> m ()
runMigration env ptbl globindices' locindices provisionMap stream =
  do
      let tbl = createTable ptbl (getProv (tableName ptbl))
          globindices = map (first adjustProv . ($ defaultprov)) globindices'
          idxattrs = concatMap snd globindices ++ concatMap snd locindices
      -- Bug in amazonka, we must not set the attribute if it is empty
      -- see https://github.com/brendanhay/amazonka/issues/332
      let final = tbl & bool (D.createTable_globalSecondaryIndexes ?~ map fst globindices) id (null globindices)
                      & bool (D.createTable_localSecondaryIndexes ?~ map fst locindices) id (null locindices)
                      & D.createTable_attributeDefinitions %~ (nub . (<> idxattrs))
                      & addStream stream
      createOrMigrate env final
  where
    getProv name = fromMaybe defaultprov (HMap.lookup name provisionMap)
    defaultprov = D.newProvisionedThroughput 5 5
    adjustProv idx = idx & (D.globalSecondaryIndex_provisionedThroughput ?~ getProv (idx ^. D.globalSecondaryIndex_indexName))
    addStream Nothing = id
    addStream (Just stype) =
      let enabled = True
          strspec = D.newStreamSpecification enabled & D.streamSpecification_streamViewType ?~ stype
      in D.createTable_streamSpecification ?~ strspec
