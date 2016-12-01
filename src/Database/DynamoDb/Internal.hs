{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}

module Database.DynamoDb.Internal where

import           Control.Monad.Supply       (Supply, evalSupply, supply)
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.Monoid                ((<>))
import           Data.Proxy
import qualified Data.Text                  as T
import           Network.AWS.DynamoDB.Types (AttributeValue)
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDb.Types

data ColumnType = TypColumn | TypSize | TypCombined
-- | Representation of a column for filter queries
-- typ - datatype of column (Int, Text..)
-- coltype - TypColumn or TypSize (result of size(column))
-- col - instance of ColumnInfo, uniquely identify a column
data Column typ (coltype :: ColumnType) col where
    Column :: Column typ 'TypColumn col
    Size :: [T.Text] -> Column Int 'TypSize col
    Combined :: [T.Text] -> Column typ 'TypCombined col

class IsColumn (a :: ColumnType)
instance IsColumn 'TypColumn
instance IsColumn 'TypCombined

-- Type of query for InCollection (we cannot query on primary key)
data QueryType = InnerQuery | OuterQuery

-- | Signifies that the column is present in the table/index
class ColumnInfo col => InCollection col tbl (query :: QueryType)

-- | Class to get a column name from a Type specifying a column
class ColumnInfo a where
  columnName :: Proxy a -> T.Text

type NameGen = Supply T.Text T.Text -> Supply T.Text (T.Text, HashMap T.Text T.Text)
nameGen :: forall typ ctyp col. ColumnInfo col => Column typ ctyp col -> NameGen
nameGen Column mkident = do
    subst <- mkident
    return (subst, HMap.fromList [(subst, columnName (Proxy :: Proxy col))])
nameGen (Size lst) mkident = do
    slist <- mapM (const mkident) lst
    return ("size(" <> T.intercalate "." slist <> ")", HMap.fromList  (zip slist lst))
nameGen (Combined lst) mkident = do
    slist <- mapM (const mkident) lst
    return (T.intercalate "." slist, HMap.fromList (zip slist lst))

-- |
data FilterCondition t =
      And (FilterCondition t) (FilterCondition t)
    | Or (FilterCondition t) (FilterCondition t)
    | Not (FilterCondition t)
    | Comparison NameGen T.Text D.AttributeValue
    | AttrExists NameGen
    | AttrMissing NameGen
    | BeginsWith NameGen D.AttributeValue
    | Contains NameGen D.AttributeValue
    | Between NameGen D.AttributeValue D.AttributeValue
    | In NameGen [D.AttributeValue]

-- | Return filter expression, attribute name map and attribute value map
dumpCondition :: FilterCondition t -> (T.Text, HashMap T.Text T.Text, HashMap T.Text D.AttributeValue)
dumpCondition fcondition = evalSupply (go fcondition) names
  where
    names = map (\i -> T.pack ("G" <> show i)) ([1..] :: [Int])
    supplyName = ("#" <>) <$> supply
    supplyValue = (":" <> ) <$> supply
    go (And cond1 cond2) = do
        (t1, a1, v1) <- go cond1
        (t2, a2, v2) <- go cond2
        return ("(" <> t1 <> ") AND (" <> t2 <> ")", a1 <> a2, v1 <> v2)
    go (Or cond1 cond2) = do
      (t1, a1, v1) <- go cond1
      (t2, a2, v2) <- go cond2
      return ("(" <> t1 <> ") OR (" <> t2 <> ")", a1 <> a2, v1 <> v2)
    go (Not cond) = do
      (t, a, v) <- go cond
      return ("NOT (" <> t <> ")", a, v)
    go (Comparison name oper val) = do
      idval <- supplyValue
      (subst, attrnames) <- name supplyName
      let expr = subst <> " " <> oper <> " " <> idval
      return (expr, attrnames, HMap.singleton idval val)
    go (Between name v1 v2) = do
      idstart <- supplyValue
      idstop <- supplyValue
      (subst, attrnames) <- name supplyName
      let expr = subst <> " BETWEEN " <> idstart <> " AND " <> idstop
          vals = HMap.fromList [(idstart, v1), (idstop, v2)]
      return (expr, attrnames, vals)

    go (In name lst) = do
        (subst, attrnames) <- name supplyName
        vlist <- mapM (\val -> (,val) <$> supplyValue) lst
        let expr = T.intercalate "," $ map fst vlist
        return (subst <> " IN (" <> expr <> ")", attrnames, HMap.fromList vlist)

    go (AttrExists name) = do
      (subst, attrnames) <- name supplyName
      let expr = "attribute_exists(" <> subst <> ")"
      return (expr, attrnames, HMap.empty)
    go (AttrMissing name) = do
      (subst, attrnames) <- name supplyName
      let expr = "attribute_not_exists(" <> subst <> ")"
      return (expr, attrnames, HMap.empty)
    go (BeginsWith name val) = do
      idval <- supplyValue
      (subst, attrnames) <- name supplyName
      let expr = "begins_with(" <> subst <> ", " <> idval <> ")"
      return (expr, attrnames, HMap.singleton idval val)
    go (Contains name val) = do
      idval <- supplyValue
      (subst, attrnames) <- name supplyName
      let expr = "contains(" <> subst <> ", " <> idval <> ")"
      return (expr, attrnames, HMap.singleton idval val)

rangeKey :: T.Text
rangeKey = ":rangekey"

rangeStart :: T.Text
rangeStart = ":rangeStart"

rangeEnd :: T.Text
rangeEnd = ":rangeEnd"

rangeOper :: RangeOper a -> T.Text -> T.Text
rangeOper (RangeEquals _) n = "#" <> n <> " = " <> rangeKey
rangeOper (RangeLessThan _) n = "#" <> n <> " < " <> rangeKey
rangeOper (RangeLessThanE _) n = "#" <> n <> " <= " <> rangeKey
rangeOper (RangeGreaterThan _) n = "#" <> n <> " > " <> rangeKey
rangeOper (RangeGreaterThanE _) n = "#" <> n <> " >= " <> rangeKey
rangeOper (RangeBetween _ _) n = "#" <> n <> " BETWEEN " <> rangeStart <> " AND " <> rangeEnd
rangeOper (RangeBeginsWith _) n = "begins_with(#" <> n <> ", " <> rangeKey <> ")"

rangeData :: DynamoScalar a => RangeOper a -> [(T.Text, AttributeValue)]
rangeData (RangeEquals a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeBetween s e) = [(rangeStart, dScalarEncode s), (rangeEnd, dScalarEncode e)]
rangeData (RangeBeginsWith a) = [(rangeKey, dScalarEncode a)]
