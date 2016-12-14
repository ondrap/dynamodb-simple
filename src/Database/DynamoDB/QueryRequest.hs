{-# LANGUAGE CPP                    #-}
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
  , queryOverIndex
  -- * Scan
  , scan
  , scanCond
  , scanSource
  -- * Query options
  , QueryOpts
  , queryOpts
  , qConsistentRead, qStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition, qLimit
  -- * Scan options
  , ScanOpts
  , scanOpts
  , sFilterCondition, sConsistentRead, sLimit, sParallel, sStartKey
) where


import           Control.Arrow              (first)
import           Control.Lens               (Lens', view, (%~), (.~), (^.))
import           Control.Lens.TH            (makeLenses)
import           Control.Monad.Catch        (throwM)
import           Data.Bool                  (bool)
import           Data.Coerce                (coerce)
import           Data.Conduit               (Conduit, Source, (=$=))
import qualified Data.Conduit.List          as CL
import           Data.Foldable              (toList)
import           Data.Function              ((&))
import           Data.HashMap.Strict        (HashMap)
import           Data.Monoid                ((<>))
import           Data.Proxy
import           Data.Sequence              (Seq)
import qualified Data.Sequence              as Seq
import qualified Data.Text                  as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.Query as D
import qualified Network.AWS.DynamoDB.Scan  as D
import qualified Network.AWS.DynamoDB.Types as D
import           Network.AWS.Pager          (AWSPager (..))
import           Numeric.Natural            (Natural)

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types
import           Database.DynamoDB.BatchRequest (getItemBatch)


-- | Helper function to decode data from the conduit.
rsDecode :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => (i -> [HashMap T.Text D.AttributeValue]) -> Conduit i m a
rsDecode trans = CL.mapFoldable trans =$= CL.mapM rsDecoder

rsDecoder :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => HashMap T.Text D.AttributeValue -> m a
rsDecoder item =
  case gsDecode item of
    Just res -> return res
    Nothing -> throwM (DynamoException $ "Error decoding item: " <> T.pack (show item))

-- | Options for a generic query.
data QueryOpts a hash range = QueryOpts {
    _qHashKey :: hash
  , _qRangeCondition :: Maybe (RangeOper range)
  , _qFilterCondition :: Maybe (FilterCondition a)
  , _qConsistentRead :: Consistency
  , _qDirection :: Direction
  , _qLimit :: Maybe Natural -- ^ This sets the "D.qLimit" settings for maximum number of evaluated items
  , _qStartKey :: Maybe (hash, range)
  -- ^ Key after which the evaluation starts. When paging, this should be set to qrsLastEvaluatedKey
  -- of the last operation.
}
makeLenses ''QueryOpts
-- | Default settings for query options.
queryOpts :: hash -> QueryOpts a hash range
queryOpts key = QueryOpts key Nothing Nothing Eventually Forward Nothing Nothing

-- | Generate a "D.Query" object
queryCmd :: forall a t v1 v2 hash range rest.
    (TableQuery a t, Code a ~ '[ hash ': range ': rest],
     DynamoScalar v1 hash, DynamoScalar v2 range)
  => QueryOpts a hash range -> D.Query
queryCmd q =
    dQueryKey (Proxy :: Proxy a) (q ^. qHashKey) (q ^. qRangeCondition)
                  & D.qConsistentRead . consistencyL .~ (q ^. qConsistentRead)
                  & D.qScanIndexForward .~ Just (q ^. qDirection == Forward)
                  & D.qLimit .~ (q ^. qLimit)
                  & addStartKey (q ^. qStartKey)
                  & addCondition (q ^. qFilterCondition)
  where
    addCondition Nothing = id
    addCondition (Just cond) =
      let (expr, attnames, attvals) = dumpCondition cond
      in (D.qExpressionAttributeNames %~ (<> attnames))
           -- HACK; https://github.com/brendanhay/amazonka/issues/332
         . bool (D.qExpressionAttributeValues %~ (<> attvals)) id (null attvals)
         . (D.qFilterExpression .~ Just expr)
    addStartKey Nothing = id
    addStartKey (Just (key, range)) =
        D.qExclusiveStartKey .~ dKeyToAttr (Proxy :: Proxy a) (key, range)

-- | When https://github.com/brendanhay/amazonka/issues/340 is fixed, remove
newtype FixedQuery = FixedQuery D.Query
instance AWSRequest FixedQuery where
  type Rs FixedQuery = D.QueryResponse
  request (FixedQuery a) = coerce (request a)
  response lgr svc _ = response lgr svc (Proxy :: Proxy D.Query)
instance AWSPager FixedQuery where
  page (FixedQuery dq) resp
    | null lastkey = Nothing
    | otherwise = Just $ FixedQuery (dq & D.qExclusiveStartKey .~ lastkey)
    where
      lastkey = resp ^. D.qrsLastEvaluatedKey

newtype FixedScan = FixedScan D.Scan
instance AWSRequest FixedScan where
  type Rs FixedScan = D.ScanResponse
  request (FixedScan a) = coerce (request a)
  response lgr svc _ = response lgr svc (Proxy :: Proxy D.Scan)
instance AWSPager FixedScan where
  page (FixedScan dq) resp
    | null lastkey = Nothing
    | otherwise = Just $ FixedScan (dq & D.sExclusiveStartKey .~ lastkey)
    where
      lastkey = resp ^. D.srsLastEvaluatedKey


-- | Generic query function. You can query table or indexes that have
-- a range key defined. The filter condition cannot access the hash and range keys.
querySource :: forall a t m v1 v2 hash range rest.
    (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
     DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a -> QueryOpts a hash range -> Source m a
querySource _ q = paginate (FixedQuery (queryCmd q)) =$= rsDecode (view D.qrsItems)

-- | Query an index, fetch primary key from the result and immediately read
-- full items from the main table.
queryOverIndex :: forall a t m v1 v2 hash hash2 r1 r2 range range2 rest rest2 parent.
    (TableQuery a t, MonadAWS m,
     Code a ~ '[ hash ': range ': rest], Code parent ~ '[ hash2 ': range2 ': rest2 ],
     DynamoIndex a parent r1, ContainsTableKey a parent (PrimaryKey parent r2),
     DynamoTable parent r2,
     DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a -> QueryOpts a hash range -> Source m parent
queryOverIndex _ q =
    paginate (FixedQuery (queryCmd q)) =$= CL.mapFoldableM batchParent
  where
    batchParent resp = do
      (vals :: [a]) <- mapM rsDecoder (resp ^. D.qrsItems)
      let keys = map dTableKey vals
      -- TODO: it would be nice if we could paginate requests from getItemBatch downstraem
      getItemBatch (q ^. qConsistentRead) keys


-- | Perform a simple, eventually consistent, query.
--
-- Simple to use function to query limited amount of data from database.
querySimple :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a -- ^ Proxy type of a table to query
  -> hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
querySimple p key range direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
  fst <$> query p opts limit

-- | Query with condition
queryCond :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a
  -> hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> FilterCondition a
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
queryCond p key range cond direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
                           & qFilterCondition .~ Just cond
  fst <$> query p opts limit

-- | Fetch exactly the required count of items even when
-- it means more calls to dynamodb. Return last evaluted key if end of data
-- was not reached. Use 'qStartKey' to continue reading the query.
query :: forall a t v1 v2 m range hash rest.
  (TableQuery a t, DynamoCollection a 'WithRange t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a
  -> QueryOpts a hash range
  -> Int -- ^ Maximum number of items to fetch
  -> m ([a], Maybe (PrimaryKey a 'WithRange))
query _ opts limit = do
    -- Add qLimit to the opts if not already there - and if there is no condition
    let cmd = queryCmd (opts & addQLimit)
    boundedFetch D.qExclusiveStartKey (view D.qrsItems) (view D.qrsLastEvaluatedKey) cmd limit
  where
    addQLimit
      | Nothing <- opts ^. qLimit, Nothing <- opts ^. qFilterCondition = qLimit .~ Just (fromIntegral limit)
      | otherwise = id

-- | Generic query interface for scanning/querying
boundedFetch :: forall a r t m range hash cmd rest.
  (MonadAWS m, HasPrimaryKey a r t, Code a ~ '[ hash ': range ': rest], AWSRequest cmd)
  => Lens' cmd (HashMap T.Text D.AttributeValue)
  -> (Rs cmd -> [HashMap T.Text D.AttributeValue])
  -> (Rs cmd -> HashMap T.Text D.AttributeValue)
  -> cmd
  -> Int -- ^ Maximum number of items to fetch
  -> m ([a], Maybe (PrimaryKey a r))
boundedFetch startLens rsResult rsLast startcmd limit = do
      (result, nextcmd) <- unfoldLimit fetch startcmd limit
      if | length result > limit ->
             let final = Seq.take limit result
             in case Seq.viewr final of
                 Seq.EmptyR -> return ([], Nothing)
                 (_ Seq.:> lastitem) -> return (toList final, Just (dItemToKey lastitem))
         | length result == limit, Just rs <- nextcmd ->
              return (toList result, dAttrToKey (Proxy :: Proxy a) (rs ^. startLens))
         | otherwise -> return (toList result, Nothing)
  where
    fetch cmd = do
        rs <- send cmd
        items <- Seq.fromList <$> mapM rsDecoder (rsResult rs)
        let lastkey = rsLast rs
            newquery = bool (Just (cmd & startLens .~ lastkey)) Nothing (null lastkey)
        return (items, newquery)

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

-- | Conduit source for running a scan.
scanSource :: (MonadAWS m, TableScan a r t, HasPrimaryKey a r t, Code a ~ '[hash ': range ': xss])
  => Proxy a -> ScanOpts a r -> Source m a
scanSource _ q = paginate (FixedScan $ scanCmd q) =$= rsDecode (view D.srsItems)

-- | Function to call bounded scans. Tries to return exactly requested number of items.
--
-- Use 'sStartKey' to continue the scan.
scan :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], TableScan a r t, HasPrimaryKey a r t)
  => Proxy a
  -> ScanOpts a r  -- ^ Scan settings
  -> Int  -- ^ Required result count
  -> m ([a], Maybe (PrimaryKey a r)) -- ^ list of results, lastEvalutedKey or Nothing if end of data reached
scan _ opts limit = do
    let cmd = scanCmd (opts & addSLimit)
    boundedFetch D.sExclusiveStartKey (view D.srsItems) (view D.srsLastEvaluatedKey) cmd limit
  where
    -- If there is no filtercondition, number of processed items = number of scanned items
    addSLimit
      | Nothing <- opts ^. sLimit, Nothing <- opts ^. sFilterCondition = sLimit .~ Just (fromIntegral limit)
      | otherwise = id

-- | Generate a "D.Query" object
scanCmd :: forall a r t hash range xss.
    (TableScan a r t, HasPrimaryKey a r t, Code a ~ '[hash ': range ': xss])
  => ScanOpts a r -> D.Scan
scanCmd q =
    dScan (Proxy :: Proxy a)
        & D.sConsistentRead . consistencyL .~ (q ^. sConsistentRead)
        & D.sLimit .~ (q ^. sLimit)
        & addStartKey (q ^. sStartKey)
        & addCondition (q ^. sFilterCondition)
        & addParallel (q ^. sParallel)
  where
    addCondition Nothing = id
    addCondition (Just cond) =
      let (expr, attnames, attvals) = dumpCondition cond
      in (D.sExpressionAttributeNames %~ (<> attnames))
           -- HACK; https://github.com/brendanhay/amazonka/issues/332
         . bool (D.sExpressionAttributeValues %~ (<> attvals)) id (null attvals)
         . (D.sFilterExpression .~ Just expr)
    --
    addStartKey Nothing = id
    addStartKey (Just pkey) = D.sExclusiveStartKey .~ dKeyToAttr (Proxy :: Proxy a) pkey
    --
    addParallel Nothing = id
    addParallel (Just (segment,total)) =
        (D.sTotalSegments .~ Just total)
        . (D.sSegment .~ Just segment)


-- | Scan table using a given filter condition.
--
-- > scanCond (colAddress <!:> "Home" <.> colCity ==. "London") 10
scanCond :: forall a m hash range rest r t.
    (MonadAWS m, HasPrimaryKey a r t, Code a ~ '[ hash ': range ': rest], TableScan a r t)
    => Proxy a -> FilterCondition a -> Int -> m [a]
scanCond _ cond limit = do
  let opts = scanOpts & sFilterCondition .~ Just cond
      cmd = scanCmd opts
  fst <$> boundedFetch D.sExclusiveStartKey (view D.srsItems) (view D.srsLastEvaluatedKey) cmd limit
