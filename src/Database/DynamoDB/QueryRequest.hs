{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ViewPatterns        #-}

module Database.DynamoDB.QueryRequest (
  -- * Query
    query
  , querySimple
  , queryCond
  , querySource
  -- * Scan
  , scan
  , scanCond
  , QueryOpts
  , queryOpts
  , qConsistentRead, qExclusiveStartKey, qDirection, qFilterCondition, qHashKey, qRangeCondition
) where

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

import           Database.DynamoDB.Class
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types


-- | Helper function to decode data from the conduit.
rsDecode :: (MonadAWS m, Code a ~ '[ hash ': range ': rest], DynamoCollection a r t)
    => (i -> [HashMap T.Text D.AttributeValue]) -> Conduit i m a
rsDecode trans = CL.mapFoldable trans =$= CL.mapM decoder
  where
    decoder item =
      case gdDecode item of
        Just res -> return res
        Nothing -> throwM (DynamoException $ "Error decoding item: " <> T.pack (show item))

-- | Options for a generic query
data QueryOpts a hash range = QueryOpts {
    _qHashKey :: hash
  , _qRangeCondition :: Maybe (RangeOper range)
  , _qFilterCondition :: Maybe (FilterCondition a)
  , _qConsistentRead :: Consistency
  , _qDirection :: Direction
  , _qLimit :: Maybe Natural -- ^ This sets the "D.qLimit" settings for maximum number of evaluated items
  , _qExclusiveStartKey :: Maybe (hash, range)
  -- ^ Key at which the evaluation starts. When paging, this should be set to qrsLastEvaluatedKey
  -- of the last operation and the first item should be dropped (??? or not ???). The qrsLastEvaluatedKey is
  -- not currently available in this API.
}
makeLenses ''QueryOpts
-- | Default settings for query options
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

query :: forall a t v1 v2 m hash range rest.
  (TableQuery a t, MonadAWS m, Code a ~ '[ hash ': range ': rest],
   DynamoScalar v1 hash, DynamoScalar v2 range)
  => QueryOpts a hash range -- ^ Consistency of the read
  -> Int -- ^ Maximum number of items to fetch
  -> m [a]
query opts limit = runConduit $ querySource opts =$= CL.take limit

data ScanOpts a = ScanOpts {

}

-- | Read full contents of a table or index.
--
-- > runConduit $ scan Eventually =$= CL.mapM_ (\(i :: Test) -> liftIO (print i))
scan :: forall a m hash range rest r t.
    (MonadAWS m, Code a ~ '[ hash ': range ': rest], TableScan a r t) => Consistency -> Source m a
scan consistency = do
    let cmd = dScan (Proxy :: Proxy a) & D.sConsistentRead .consistencyL .~ consistency
    paginate cmd =$= rsDecode (view D.srsItems)

-- | Scan table using a given filter condition.
--
-- > runConduit $ scanCond Eventually (colAddress <!:> "Home" <.> colCity ==. "London")
-- >          =$= CL.mapM_ (\(i :: Test) -> liftIO (print i))
scanCond :: forall a m hash range rest r t.
    (MonadAWS m, Code a ~ '[ hash ': range ': rest], TableScan a r t)
    => Consistency -> FilterCondition a -> Source m a
scanCond consistency cond = do
    let (expr, attnames, attvals) = dumpCondition cond
        cmd = dScan (Proxy :: Proxy a) & D.sConsistentRead . consistencyL .~ consistency
                                       & D.sExpressionAttributeNames .~ attnames
                                       & bool (D.sExpressionAttributeValues .~ attvals) id (null attvals) -- HACK; https://github.com/brendanhay/amazonka/issues/332
                                       & D.sFilterExpression .~ Just expr
    paginate cmd =$= rsDecode (view D.srsItems)
