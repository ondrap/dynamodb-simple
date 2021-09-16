{-# LANGUAGE CPP                 #-}
#if __GLASGOW_HASKELL__ >= 800
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
#endif
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.DynamoDB.QueryRequest (
  -- * Query
    query
  , querySimple
  , queryCond
  , querySource
  , querySourceChunks
  , queryOverIndex
  , querySourceByKey
  -- * Scan
  , scan
  , scanCond
  , scanSource
  , scanSourceChunks
  -- * Query options
  , QueryOpts
  , queryOpts
  , qConsistentRead, qStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition, qLimit
  -- * Scan options
  , ScanOpts
  , scanOpts
  , sFilterCondition, sConsistentRead, sLimit, sParallel, sStartKey
  -- * Helper conduits
  , leftJoin
  , innerJoin
) where


import           Control.Arrow                  (first, second)
import           Control.Lens                   (Lens', sequenceOf, view, (%~),
                                                 (.~), (?~), (^.), _2)
import           Control.Lens.TH                (makeLenses)
import           Control.Monad                  ((>=>))
import           Control.Monad.Catch            (throwM)
import           Control.Monad.Trans.Resource
import           Data.Bool                      (bool)
import           Data.Conduit                   (Conduit, Source, (=$=))
import qualified Data.Conduit.List              as CL
import           Data.Foldable                  (toList)
import           Data.Function                  ((&))
import           Data.HashMap.Strict            (HashMap)
import qualified Data.Map                       as Map
import           Data.Maybe                     (mapMaybe, fromMaybe)
import           Data.Monoid                    ((<>))
import           Data.Proxy
import           Data.Sequence                  (Seq)
import qualified Data.Sequence                  as Seq
import qualified Data.Text                      as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.Query     as D
import qualified Network.AWS.DynamoDB.Scan      as D
import qualified Network.AWS.DynamoDB.Types     as D
import           Numeric.Natural                (Natural)
import qualified Data.HashMap.Strict as HMap

import           Database.DynamoDB.BatchRequest (getItemBatch)
import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types

-- | Decode data, throw exception if decoding fails.
rsDecoder :: (DynamoCollection a r t, MonadResource m, MonadThrow m)
    => HashMap T.Text D.AttributeValue -> m a
rsDecoder item =
  case dGsDecode item of
    Right res -> return res
    Left err -> throwM (DynamoException $ "Error decoding item: " <> err)

-- | Options for a generic query.
data QueryOpts a hash range = QueryOpts {
    _qHashKey :: hash
  , _qRangeCondition :: Maybe (RangeOper range)
  , _qFilterCondition :: Maybe (FilterCondition a)
  , _qConsistentRead :: Consistency
  , _qDirection :: Direction
  , _qLimit :: Maybe Natural -- ^ This sets the "D.query_limit" settings for maximum number of evaluated items
  , _qStartKey :: Maybe (hash, range)
  -- ^ Key after which the evaluation starts. When paging, this should be set to queryResponse_lastEvaluatedKey
  -- of the last operation.
}
makeLenses ''QueryOpts
-- | Default settings for query options.
queryOpts :: hash -> QueryOpts a hash range
queryOpts key = QueryOpts key Nothing Nothing Eventually Forward Nothing Nothing

-- | Generate a "D.Query" object
queryCmd :: forall a t hash range. CanQuery a t hash range => QueryOpts a hash range -> D.Query
queryCmd q =
    dQueryKey (Proxy :: Proxy a) (q ^. qHashKey) (q ^. qRangeCondition)
                  & D.query_consistentRead . consistencyL .~ (q ^. qConsistentRead)
                  & D.query_scanIndexForward ?~ (q ^. qDirection == Forward)
                  & D.query_limit .~ (q ^. qLimit)
                  & addStartKey (q ^. qStartKey)
                  & addCondition (q ^. qFilterCondition)
  where
    addCondition Nothing = id
    addCondition (Just cond) =
      let (expr, attnames, attvals) = dumpCondition cond
      in (D.query_expressionAttributeNames %~ Just . (<> attnames) . fromMaybe mempty)
          -- HACK; https://github.com/brendanhay/amazonka/issues/332
         . bool (D.query_expressionAttributeValues %~ Just . (<> attvals) . fromMaybe mempty) id (null attvals)
         . (D.query_filterExpression ?~ expr)
    addStartKey Nothing = id
    addStartKey (Just (key, range)) =
        D.query_exclusiveStartKey ?~ dQueryKeyToAttr (Proxy :: Proxy a) (key, range)

-- | Same as 'querySource', but return data in original chunks.
querySourceChunks :: forall a t m hash range. (CanQuery a t hash range, MonadResource m, MonadThrow m)
  => Env -> Proxy a -> QueryOpts a hash range -> Source m [a]
querySourceChunks env _ q = paginate env (queryCmd q) =$= CL.mapM (\res -> mapM rsDecoder (fromMaybe mempty (res ^. D.queryResponse_items)))

-- | Generic query function. You can query table or indexes that have
-- a range key defined. The filter condition cannot access the hash and range keys.
querySource :: forall a t m hash range. (CanQuery a t hash range, MonadResource m, MonadThrow m)
  => Env -> Proxy a -> QueryOpts a hash range -> Source m a
querySource env p q = querySourceChunks env p q =$= CL.concat


querySourceChunksByKey :: forall a parent hash rest v1 m r.
  (DynamoIndex a parent 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v1 hash,
   DynamoTable parent r, MonadResource m, MonadThrow m)
  => Env -> Proxy a -> hash -> Source m [a]
querySourceChunksByKey env p key = paginate env sQuery =$= CL.mapM (\res -> mapM rsDecoder (fromMaybe mempty (res ^. D.queryResponse_items)))
  where
    sQuery = D.newQuery (tableName (Proxy :: Proxy parent))
                                      & D.query_keyConditionExpression ?~ "#K = :key"
                                      & D.query_expressionAttributeNames ?~ HMap.singleton "#K" (head (allFieldNames p))
                                      & D.query_expressionAttributeValues ?~ HMap.singleton ":key" (dScalarEncode key)
                                      & D.query_indexName ?~ indexName p

-- | Conduit to query global indexes with no range key; in case anyone needed it
querySourceByKey :: forall a parent hash rest v1 m r.
  (DynamoIndex a parent 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v1 hash,
   DynamoTable parent r, MonadResource m, MonadThrow m)
  => Env -> Proxy a -> hash -> Source m a
querySourceByKey env p q = querySourceChunksByKey env p q =$= CL.concat

-- | Query an index, fetch primary key from the result and immediately read
-- full items from the main table.
--
-- You cannot perform strongly consistent reads on Global indexes; if you set
-- the 'qConsistentRead' to 'Strongly', fetch from global indexes is still done
-- as eventually consistent. Queries on local indexes are performed according to the settings.
queryOverIndex :: forall a t m v1 v2 hash r2 range rest parent.
    (CanQuery a t hash range,
     Code a ~ '[ hash ': range ': rest],
     DynamoIndex a parent 'WithRange, ContainsTableKey a parent (PrimaryKey parent r2),
     DynamoTable parent r2,
     DynamoScalar v1 hash, DynamoScalar v2 range,
     MonadThrow m,
     MonadResource m)
  => Env -> Proxy a -> QueryOpts a hash range -> Source m parent
queryOverIndex env p q =
   querySourceChunks env p (q & setConsistency)
      =$= CL.mapFoldableM batchParent
  where
    setConsistency -- Strong consistent reads not supported on global indexes
      | indexIsLocal p = id
      | otherwise = qConsistentRead .~ Eventually
    batchParent vals = getItemBatch env (q ^. qConsistentRead) (map dTableKey vals)

-- | Perform a simple, eventually consistent, query.
--
-- Simple to use function to query limited amount of data from database.
querySimple :: forall a t m hash range.
  (CanQuery a t hash range, MonadResource m, MonadThrow m)
  => Env
  -> Proxy a -- ^ Proxy type of a table to query
  -> hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
querySimple env p key range direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
  fst <$> query env p opts limit

-- | Query with condition
queryCond :: forall a t m hash range.
  (CanQuery a t hash range, MonadResource m, MonadThrow m)
  => Env
  -> Proxy a
  -> hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> FilterCondition a
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
queryCond env p key range cond direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
                           & qFilterCondition ?~ cond
  fst <$> query env p opts limit

-- | Fetch exactly the required count of items even when
-- it means more calls to dynamodb. Return last evaluted key if end of data
-- was not reached. Use 'qStartKey' to continue reading the query.
query :: forall a t m range hash.
  (CanQuery a t hash range, MonadResource m, MonadThrow m)
  => Env
  -> Proxy a
  -> QueryOpts a hash range
  -> Int -- ^ Maximum number of items to fetch
  -> m ([a], Maybe (PrimaryKey a 'WithRange))
query env _ opts limit = do
    -- Add query_limit to the opts if not already there - and if there is no condition
    let cmd = queryCmd (opts & addQLimit)
    boundedFetch env D.query_exclusiveStartKey (fromMaybe mempty . view D.queryResponse_items) (fromMaybe mempty . view D.queryResponse_lastEvaluatedKey) cmd limit
  where
    addQLimit
      | Nothing <- opts ^. qLimit, Nothing <- opts ^. qFilterCondition = qLimit ?~ fromIntegral limit
      | otherwise = id

-- | Generic query interface for scanning/querying
boundedFetch :: forall a r t m cmd.
  (HasPrimaryKey a r t, AWSRequest cmd, MonadResource m, MonadThrow m)
  => Env
  -> Lens' cmd (Maybe (HashMap T.Text D.AttributeValue))
  -> (AWSResponse cmd -> [HashMap T.Text D.AttributeValue])
  -> (AWSResponse cmd -> HashMap T.Text D.AttributeValue)
  -> cmd
  -> Int -- ^ Maximum number of items to fetch
  -> m ([a], Maybe (PrimaryKey a r))
boundedFetch env startLens rsResult rsLast startcmd limit = do
      (result, nextcmd) <- unfoldLimit fetch startcmd limit
      if | length result > limit ->
             let final = Seq.take limit result
             in case Seq.viewr final of
                 Seq.EmptyR -> return ([], Nothing)
                 (_ Seq.:> lastitem) -> return (toList final, Just (dItemToKey lastitem))
         | length result == limit, Just rs <- nextcmd ->
              return (toList result, dAttrToKey (Proxy :: Proxy a) (fromMaybe mempty (rs ^. startLens)))
         | otherwise -> return (toList result, Nothing)
  where
    fetch cmd = do
        rs <- send env cmd
        items <- Seq.fromList <$> mapM rsDecoder (rsResult rs)
        let lastkey = rsLast rs
            newquery = bool (Just (cmd & startLens ?~ lastkey)) Nothing (null lastkey)
        return (items, newquery)

-- | Run command as long as Maybe cmd is Just or the resulting sequence is smaller than limit
unfoldLimit :: Monad m => (cmd -> m (Seq a, Maybe cmd)) -> cmd -> Int -> m (Seq a, Maybe cmd)
unfoldLimit code = go
  where
    go cmd limit = do
      (vals, mnext) <- code cmd
      let cnt = length vals
      if | Just next <- mnext, cnt < limit -> first (vals <>) <$> go next (limit - cnt)
         | otherwise                       -> return (vals, mnext)

-- | Record for defining scan command. Use lenses to set the content.
--
-- sParallel: (Segment number, Total segments)
data ScanOpts a r = ScanOpts {
    _sFilterCondition :: Maybe (FilterCondition a)
  , _sConsistentRead :: Consistency
  , _sLimit :: Maybe Natural
  , _sParallel :: Maybe (Natural, Natural) -- ^ (Segment number, TotalSegments)
  , _sStartKey :: Maybe (PrimaryKey a r)
}
makeLenses ''ScanOpts
scanOpts :: ScanOpts a r
scanOpts = ScanOpts Nothing Eventually Nothing Nothing Nothing

-- | Conduit source for running scan; the same as 'scanSource', but return results in chunks as they come.
scanSourceChunks :: (TableScan a r t, MonadResource m, MonadThrow m) => Env -> Proxy a -> ScanOpts a r -> Source m [a]
scanSourceChunks env _ q = paginate env (scanCmd q) =$= CL.mapM (\res -> mapM rsDecoder (fromMaybe mempty (res ^. D.scanResponse_items)))

-- | Conduit source for running a scan.
scanSource :: (TableScan a r t, MonadResource m, MonadThrow m) => Env -> Proxy a -> ScanOpts a r -> Source m a
scanSource env p q = scanSourceChunks env p q =$= CL.concat

-- | Function to call bounded scans. Tries to return exactly requested number of items.
--
-- Use 'sStartKey' to continue the scan.
scan :: (TableScan a r t, MonadResource m, MonadThrow m)
  => Env
  -> Proxy a
  -> ScanOpts a r  -- ^ Scan settings
  -> Int  -- ^ Required result count
  -> m ([a], Maybe (PrimaryKey a r)) -- ^ list of results, lastEvalutedKey or Nothing if end of data reached
scan env _ opts limit = do
    let cmd = scanCmd (opts & addSLimit)
    boundedFetch env D.scan_exclusiveStartKey (fromMaybe mempty . view D.scanResponse_items) (fromMaybe mempty . view D.scanResponse_lastEvaluatedKey) cmd limit
  where
    -- If there is no filtercondition, number of processed items = number of scanned items
    addSLimit
      | Nothing <- opts ^. sLimit, Nothing <- opts ^. sFilterCondition = sLimit ?~ fromIntegral limit
      | otherwise = id

-- | Generate a "D.Query" object.
scanCmd :: forall a r t. TableScan a r t => ScanOpts a r -> D.Scan
scanCmd q =
    defaultScan (Proxy :: Proxy a)
        & D.scan_consistentRead . consistencyL .~ (q ^. sConsistentRead)
        & D.scan_limit .~ (q ^. sLimit)
        & addStartKey (q ^. sStartKey)
        & addCondition (q ^. sFilterCondition)
        & addParallel (q ^. sParallel)
  where
    addCondition Nothing = id
    addCondition (Just cond) =
      let (expr, attnames, attvals) = dumpCondition cond
      in (D.scan_expressionAttributeNames %~ Just . (<> attnames) . fromMaybe mempty)
          -- HACK; https://github.com/brendanhay/amazonka/issues/332
         . bool (D.scan_expressionAttributeValues %~ Just . (<> attvals) . fromMaybe mempty) id (null attvals)
         . (D.scan_filterExpression ?~ expr)
    --
    addStartKey Nothing = id
    addStartKey (Just pkey) = D.scan_exclusiveStartKey ?~ dKeyToAttr (Proxy :: Proxy a) pkey
    --
    addParallel Nothing = id
    addParallel (Just (segment,total)) =
        (D.scan_totalSegments ?~ total)
        . (D.scan_segment ?~ segment)


-- | Scan table using a given filter condition.
--
-- > scanCond (colAddress <!:> "Home" <.> colCity ==. "London") 10
scanCond :: forall a m r t. (TableScan a r t, MonadResource m, MonadThrow m) => Env -> Proxy a -> FilterCondition a -> Int -> m [a]
scanCond env _ cond limit = do
  let opts = scanOpts & sFilterCondition ?~ cond
      cmd = scanCmd opts
  fst <$> boundedFetch env D.scan_exclusiveStartKey (fromMaybe mempty . view D.scanResponse_items) (fromMaybe mempty . view D.scanResponse_lastEvaluatedKey) cmd limit

-- | Conduit to do a left join on the items being sent; supposed to be used with querySourceChunks.
--
-- The 'foreign key' must have an 'Ord' to facilitate faster searching.
leftJoin :: forall a b m r.
    (DynamoTable a r, Ord (PrimaryKey a r), ContainsTableKey a a (PrimaryKey a r), MonadThrow m, MonadResource m)
    => Env
    -> Consistency
    -> Proxy a -- ^ Proxy type for the right table
    -> (b -> Maybe (PrimaryKey a r))
    -> Conduit [b] m [(b, Maybe a)]
leftJoin env consistency p getkey = CL.mapM doJoin
  where
    doJoin input = do
      let keys = filter (dKeyIsDefined p) $ mapMaybe getkey input
      rightTbl <- getItemBatch env consistency keys
      let resultMap = Map.fromList $ map (\res -> (dTableKey res,res)) rightTbl
      return $ map (second (id >=> (`Map.lookup` resultMap))) $ zip input $ map getkey input

-- | The same as 'leftJoin', but discard items that do not exist in the right table.
innerJoin :: forall a b m r.
    (DynamoTable a r, Ord (PrimaryKey a r), ContainsTableKey a a (PrimaryKey a r), MonadThrow m, MonadResource m)
    => Env
    -> Consistency
    -> Proxy a -- ^ Proxy type for the right table
    -> (b -> Maybe (PrimaryKey a r))
    -> Conduit [b] m [(b, a)]
innerJoin env consistency p getkey =
    leftJoin env consistency p getkey =$= CL.map (mapMaybe (sequenceOf _2))
