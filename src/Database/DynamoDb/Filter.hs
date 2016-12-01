{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}

module Database.DynamoDb.Filter (
      FilterCondition(Not)
    , (&&.), (||.)
    , (==.), (/=.), (>=.), (>.), (<=.), (<.)
    , (<.>)
    , attrExists, attrMissing, beginsWith, contains, tcontains, valIn, between
    , size
    , Column(Column)
    , TypColumn
    , dumpCondition
    , InCollection
    , ColumnInfo(..)
    , InnerQuery, OuterQuery
) where

import           Control.Lens               ((.~), (^.))
import           Control.Monad.Supply       (evalSupply, supply, Supply)
import           Data.Function              ((&))
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.Maybe                 (fromMaybe)
import           Data.Monoid                ((<>))
import           Data.Proxy
import qualified Data.Set                   as Set
import qualified Data.Text                  as T
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDb.Types

data TypColumn
data TypSize
data TypCombined
-- | Representation of a column for filter queries
-- typ - datatype of column (Int, Text..)
-- coltype - TypColumn or TypSize (result of size(column))
-- col - instance of ColumnInfo, uniquely identify a column
data Column typ coltype col where
    Column :: Column typ TypColumn col
    Size :: [T.Text] -> Column Int TypSize col
    Combined :: [T.Text] -> Column typ TypCombined col

(<.>) :: forall typ col1 typ2 col2 ct2.
        (InCollection col2 typ InnerQuery, ColumnInfo col1, ColumnInfo col2, IsColumn ct2)
      => Column typ TypColumn col1 -> Column typ2 ct2 col2 -> Column typ2 TypCombined col1
(<.>) Column Column = Combined [columnName (Proxy :: Proxy col1), columnName (Proxy :: Proxy col2)]
(<.>) Column (Combined other) = Combined (columnName (Proxy :: Proxy col1) : other)
(<.>) Column (Size _) = error "This cannot happen <.>"
-- We need to associate from the right
infixr 7 <.>

class IsColumn a
instance IsColumn TypColumn
instance IsColumn TypCombined

-- Type of query for InCollection (we cannot query on primary key)
data InnerQuery
data OuterQuery

-- | Signifies that the column is present in the table/index
class ColumnInfo col => InCollection col tbl query

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
data FilterCondition t q =
      And (FilterCondition t q) (FilterCondition t q)
    | Or (FilterCondition t q) (FilterCondition t q)
    | Not (FilterCondition t q)
    | Comparison NameGen T.Text D.AttributeValue
    | AttrExists NameGen
    | AttrMissing NameGen
    | BeginsWith NameGen D.AttributeValue
    | Contains NameGen D.AttributeValue
    | Between NameGen D.AttributeValue D.AttributeValue
    | In NameGen [D.AttributeValue]

-- | Return filter expression, attribute name map and attribute value map
dumpCondition :: FilterCondition t OuterQuery -> (T.Text, HashMap T.Text T.Text, HashMap T.Text D.AttributeValue)
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

between :: (Ord typ, InCollection col tbl q, DynamoEncodable typ)
  => Column typ ctyp col -> typ -> typ -> FilterCondition tbl q
between col a b = Between (nameGen col) (dScalarEncode a) (dScalarEncode b)

valIn :: (InCollection col tbl q, DynamoEncodable typ)
  => Column typ ctyp col -> [typ] -> FilterCondition tbl q
valIn col lst = In (nameGen col) (map dScalarEncode lst)

attrExists :: (InCollection col tbl q, IsColumn ct) => Column typ ct col -> FilterCondition tbl q
attrExists col = AttrExists (nameGen col)

attrMissing :: (InCollection col tbl q, IsColumn ct) => Column typ ct col -> FilterCondition tbl q
attrMissing col = AttrMissing (nameGen col)

beginsWith :: (InCollection col tbl q, IsText typ, IsColumn ct)
  => Column typ ct col -> T.Text -> FilterCondition tbl q
beginsWith col txt = BeginsWith (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for rext-like attributes
tcontains :: (InCollection col tbl q, IsText typ, IsColumn ct)
  => Column typ ct col -> T.Text -> FilterCondition tbl q
tcontains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for sets
contains :: (InCollection col tbl q, IsColumn ct, DynamoEncodable a)
  => Column (Set.Set a) ct col -> a -> FilterCondition tbl q
contains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | Size (i.e. number of bytes) of saved attribute
size :: forall typ col ct. (ColumnInfo col, IsColumn ct) => Column typ ct col -> Column Int TypSize col
size Column = Size [columnName (Proxy :: Proxy col)]
size (Combined lst) = Size lst
size (Size _) = error "This cannot happen - size"

dcomp :: (InCollection col tbl q, DynamoEncodable typ)
  => T.Text -> Column typ ctyp col -> typ -> FilterCondition tbl q
dcomp op col val = Comparison (nameGen col) op encval
  where
    -- Ord comparing against nothing doesn't make much sense - failback to NULL
    encval = fromMaybe (D.attributeValue & D.avNULL .~ Just True) (dEncode val)

(&&.) :: FilterCondition t q -> FilterCondition t q -> FilterCondition t q
(&&.) = And
infixr 3 &&.

(||.) :: FilterCondition t q -> FilterCondition t q -> FilterCondition t q
(||.) = Or
infixr 3 ||.

(==.) :: (InCollection col tbl q, DynamoEncodable typ)
  => Column typ ctyp col -> typ -> FilterCondition tbl q
(==.) col val =
  case dEncode val of
    -- Hack to have '==. Nothing' correctly working
    Nothing -> AttrMissing (nameGen col)
    -- Hack for '==. ""' or empty set to work correctly on non-initialized values
    Just encval | encval ^. D.avNULL == Just True ->
                          AttrMissing (nameGen col) ||. Comparison (nameGen col) "=" encval
                | otherwise -> Comparison (nameGen col) "=" encval
infix 4 ==.

(/=.) :: (InCollection col tbl q, DynamoEncodable typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl q
(/=.) col val = Not (dcomp "=" col val)
infix 4 /=.


(<=.) :: (InCollection col tbl q, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl q
(<=.) = dcomp "<="
infix 4 <=.

(<.) :: (InCollection col tbl q, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl q
(<.) = dcomp "<"
infix 4 <.

(>.) :: (InCollection col tbl q, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl q
(>.) = dcomp ">"
infix 4 >.

(>=.) :: (InCollection col tbl q, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl q
(>=.) = dcomp ">="
infix 4 >=.
