{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ViewPatterns        #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE FlexibleContexts          #-}

module Database.DynamoDB.QueryRequest (
  -- * Query
    query
  , querySimple
  , queryCond
  , querySource
  -- * Scan
  , scanCond
  , scanSource
  -- * Query options
  , QueryOpts
  , queryOpts
  , qConsistentRead, qExclusiveStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition, qLimit
  -- * Scan options
  , ScanOpts
  , scanOpts
  , sFilterCondition, sConsistentRead, sLimit, sParallel, sExclusiveStartKey
) where


import           Control.Arrow                   (first)
import           Control.Lens                    (view, (%~), (.~), (^.))
import           Control.Lens.TH                 (makeLenses)
import           Control.Monad.Catch             (throwM)
import           Data.Bool                       (bool)
import           Data.Conduit                    (Conduit, Source, runConduit,
                                                  (=$=))
import qualified Data.Conduit.List               as CL
import           Data.Function                   ((&))
import           Data.HashMap.Strict             (HashMap)
import           Data.Monoid                     ((<>))
import           Data.Proxy
import qualified Data.Text                       as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.Query      as D
import qualified Network.AWS.DynamoDB.Scan       as D
import qualified Network.AWS.DynamoDB.Types      as D
import           Numeric.Natural                 (Natural)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Foldable (toList)

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types


-- | Helper function to decode data from the conduit.
rsDecode :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => (i -> [HashMap T.Text D.AttributeValue]) -> Conduit i m a
rsDecode trans = CL.mapFoldable trans =$= CL.mapM rsDecoder

rsDecoder :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => HashMap T.Text D.AttributeValue -> m a
rsDecoder item =
  case gdDecode item of
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
  , _qExclusiveStartKey :: Maybe (hash, range)
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
                  & addStartKey (q ^. qExclusiveStartKey)
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
        D.qExclusiveStartKey .~ dKeyAndAttr (Proxy :: Proxy a) (key, range)

-- | Generic query function. You can query table or indexes that have
-- a range key defined. The filter condition cannot access the hash and range keys.
-- Note: see https://github.com/brendanhay/amazonka/issues/340
querySource :: forall a t m v1 v2 hash range rest.
    (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
     DynamoScalar v1 hash, DynamoScalar v2 range)
  => QueryOpts a hash range -> Source m a
querySource q = paginate (queryCmd q) =$= rsDecode (view D.qrsItems)

-- | Perform a simple, eventually consistent, query.
--
-- Simple to use function to query limited amount of data from database.
--
-- Throws 'DynamoException' if an item cannot be decoded.
querySimple :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
querySimple key range direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
                           -- Without the condition, the number of processed and returned items
                           -- should be roughly the same
                           & qLimit .~ Just (fromIntegral limit)
  runConduit $ querySource opts =$= CL.take limit

-- | Query with condition
-- Note: see https://github.com/brendanhay/amazonka/issues/340
queryCond :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => hash        -- ^ Hash key
  -> Maybe (RangeOper range) -- ^ Range condition
  -> FilterCondition a
  -> Direction -- ^ Scan direction
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
queryCond key range cond direction limit = do
  let opts = queryOpts key & qRangeCondition .~ range
                           & qDirection .~ direction
                           & qFilterCondition .~ Just cond
  runConduit $ querySource opts =$= CL.take limit

-- | Simple query interface; tries to fetch exactly the required count of items even when
-- it means more calls to dynamodb. Return last evaluted key if end of data
-- was not reached.
query :: forall a t v1 v2 m range hash rest.
  (TableQuery a t, DynamoCollection a 'WithRange t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => QueryOpts a hash range
  -> Int -- ^ Maximum number of items to fetch
  -> m ([a], Maybe (PrimaryKey (Code a) 'WithRange))
query opts limit = do
      (result, nextcmd) <- unfoldLimit fetch (queryCmd opts) limit
      if | length result > limit ->
             let final = Seq.take limit result
             in case Seq.viewr final of
                 Seq.EmptyR -> return ([], Nothing)
                 (_ Seq.:> lastitem) -> return (toList final, Just (dItemToKey lastitem))
         | length result == limit, Just rs <- nextcmd ->
              return (toList result, dAttrToKey (Proxy :: Proxy a) (rs ^. D.qExclusiveStartKey))
         | otherwise -> return (toList result, Nothing)
  where
    fetch cmd = do
        rs <- send cmd
        items <- Seq.fromList <$> mapM rsDecoder (rs ^. D.qrsItems)
        let lastkey = rs ^. D.qrsLastEvaluatedKey
            newquery = bool (Just (cmd & D.qExclusiveStartKey .~ lastkey)) Nothing (null lastkey)
        return (items, newquery)

unfoldLimit :: Monad m => (D.Query -> m (Seq a, Maybe D.Query)) -> D.Query -> Int -> m (Seq a, Maybe D.Query)
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
  , _sExclusiveStartKey :: Maybe (PrimaryKey (Code a) r)
}
makeLenses ''ScanOpts
scanOpts :: ScanOpts a r
scanOpts = ScanOpts Nothing Eventually Nothing Nothing Nothing

-- | Conduit source for running a scan.
scanSource :: (MonadAWS m, TableScan a r t, HasPrimaryKey a r t, Code a ~ '[hash ': range ': xss])
  => ScanOpts a r -> Source m a
scanSource q = paginate (scanCmd q) =$= rsDecode (view D.srsItems)

-- | Generate a "D.Query" object
scanCmd :: forall a r t hash range xss.
    (TableScan a r t, HasPrimaryKey a r t, Code a ~ '[hash ': range ': xss])
  => ScanOpts a r -> D.Scan
scanCmd q =
    dScan (Proxy :: Proxy a)
        & D.sConsistentRead . consistencyL .~ (q ^. sConsistentRead)
        & D.sLimit .~ (q ^. sLimit)
        & addStartKey (q ^. sExclusiveStartKey)
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
    addStartKey (Just pkey) = D.sExclusiveStartKey .~ dKeyAndAttr (Proxy :: Proxy a) pkey
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
    => FilterCondition a -> Int -> m [a]
scanCond cond limit = do
  let opts = scanOpts & sFilterCondition .~ Just cond
  runConduit $ scanSource opts =$= CL.take limit
