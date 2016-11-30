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
    , (==.), (>=.), (>.), (<=.), (<.)
    , (<.>)
    , attrExists, attrMissing, beginsWith, contains, tcontains, valIn, between
    , size
    , Column(Column)
    , TypColumn
    , dumpCondition
    , InCollection
    , ColumnInfo(..)
) where

import           Control.Monad.Supply       (evalSupply, supply)
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
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
    Size :: T.Text -> Column Int TypSize col
    Combined :: T.Text -> Column typ TypCombined col

(<.>) :: forall typ col1 typ2 col2 ct2.
        (InCollection col2 typ, ColumnInfo col1, ColumnInfo col2, IsColumn ct2)
      => Column typ TypColumn col1 -> Column typ2 ct2 col2 -> Column typ2 TypCombined col1
(<.>) Column Column = Combined (columnName (Proxy :: Proxy col1) <> "." <> columnName (Proxy :: Proxy col2))
(<.>) Column (Combined cname) = Combined (columnName (Proxy :: Proxy col1) <> "." <> cname)
(<.>) Column (Size _) = error "This cannot happen <.>"
-- We need to associate from the right
infixr 7 <.>

class IsColumn a
instance IsColumn TypColumn
instance IsColumn TypCombined

-- | Signifies that the column is present in the table/index
class ColumnInfo col => InCollection col tbl

-- | Class to get a column name from a Type specifying a column
class ColumnInfo a where
  columnName :: Proxy a -> T.Text

type NameGen = T.Text -> (T.Text, T.Text)
nameGen :: forall typ ctyp col. ColumnInfo col => Column typ ctyp col -> NameGen
nameGen Column subst = (subst, columnName (Proxy :: Proxy col))
nameGen (Size txt) subst = ("size(" <> subst <> ")", txt)
nameGen (Combined txt) subst = (subst, txt)

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
      ident <- supplyName
      idval <- supplyValue
      let (subst, colname) = name ident
          expr = subst <> " " <> oper <> " " <> idval
      return (expr, HMap.singleton ident colname, HMap.singleton idval val)
    go (Between name v1 v2) = do
      idname <- supplyName
      idstart <- supplyValue
      idstop <- supplyValue
      let (subst, colname) = name idname
          expr = subst <> " BETWEEN " <> idstart <> " AND " <> idstop
          vals = HMap.fromList [(idstart, v1), (idstop, v2)]
      return (expr, HMap.singleton idname colname, vals)

    go (In name lst) = do
        idname <- supplyName
        let (subst, colname) = name idname
        vlist <- mapM (\val -> (,val) <$> supplyValue) lst
        let expr = T.intercalate "," $ map fst vlist
        return (subst <> " IN (" <> expr <> ")", HMap.singleton idname colname, HMap.fromList vlist)

    go (AttrExists name) = do
      ident <- supplyName
      let (subst, colname) = name ident
          expr = "attribute_exists(" <> subst <> ")"
      return (expr, HMap.singleton ident colname, HMap.empty)
    go (AttrMissing name) = do
      ident <- supplyName
      let (subst, colname) = name ident
          expr = "attribute_not_exists(" <> subst <> ")"
      return (expr, HMap.singleton ident colname, HMap.empty)
    go (BeginsWith name val) = do
      ident <- supplyName
      idval <- supplyValue
      let (subst, colname) = name ident
          expr = "begins_with(" <> subst <> ", " <> idval <> ")"
      return (expr, HMap.singleton ident colname, HMap.singleton idval val)
    go (Contains name val) = do
      ident <- supplyName
      idval <- supplyValue
      let (subst, colname) = name ident
          expr = "contains(" <> subst <> ", " <> idval <> ")"
      return (expr, HMap.singleton ident colname, HMap.singleton idval val)

between :: (Ord typ, InCollection col tbl, DynamoEncodable typ) => Column typ ctyp col -> typ -> typ -> FilterCondition tbl
between col a b = Between (nameGen col) (dScalarEncode a) (dScalarEncode b)

valIn :: (InCollection col tbl, DynamoEncodable typ) => Column typ ctyp col -> [typ] -> FilterCondition tbl
valIn col lst = In (nameGen col) (map dScalarEncode lst)

attrExists :: (InCollection col tbl, IsColumn ct) => Column typ ct col -> FilterCondition tbl
attrExists col = AttrExists (nameGen col)

attrMissing :: (InCollection col tbl, IsColumn ct) => Column typ ct col -> FilterCondition tbl
attrMissing col = AttrMissing (nameGen col)

beginsWith :: (InCollection col tbl, IsText typ, IsColumn ct) => Column typ ct col -> T.Text -> FilterCondition tbl
beginsWith col txt = BeginsWith (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for rext-like attributes
tcontains :: (InCollection col tbl, IsText typ, IsColumn ct) => Column typ ct col -> T.Text -> FilterCondition tbl
tcontains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for sets
contains :: (InCollection col tbl, IsColumn ct, DynamoEncodable a) => Column (Set.Set a) ct col -> a -> FilterCondition tbl
contains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | Size (i.e. number of bytes) of saved attribute
size :: forall typ col ct. (ColumnInfo col, IsColumn ct) => Column typ ct col -> Column Int TypSize col
size Column = Size (columnName (Proxy :: Proxy col))
size (Combined txt) = Size txt
size (Size _) = error "This cannot happen - size"

dcomp :: (InCollection col tbl, DynamoEncodable typ) => T.Text -> Column typ ctyp col -> typ -> FilterCondition tbl
dcomp op col val = Comparison (nameGen col) op (dScalarEncode val)

(&&.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(&&.) = And
infixr 3 &&.

(||.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(||.) = Or
infixr 3 ||.

(==.) :: (InCollection col tbl, DynamoEncodable typ) => Column typ ctyp col -> typ -> FilterCondition tbl
(==.) col val
  | dIsNothing val = AttrMissing (nameGen col) -- Hack to have '==. Nothing' correctly working
  | otherwise = dcomp "=" col val
infix 4 ==.

(<=.) :: (InCollection col tbl, DynamoEncodable typ, Ord typ) => Column typ ctyp col -> typ -> FilterCondition tbl
(<=.) = dcomp "<="
infix 4 <=.

(<.) :: (InCollection col tbl, DynamoEncodable typ, Ord typ) => Column typ ctyp col -> typ -> FilterCondition tbl
(<.) = dcomp "<"
infix 4 <.

(>.) :: (InCollection col tbl, DynamoEncodable typ, Ord typ) => Column typ ctyp col -> typ -> FilterCondition tbl
(>.) = dcomp ">"
infix 4 >.

(>=.) :: (InCollection col tbl, DynamoEncodable typ, Ord typ) => Column typ ctyp col -> typ -> FilterCondition tbl
(>=.) = dcomp ">="
infix 4 >=.
