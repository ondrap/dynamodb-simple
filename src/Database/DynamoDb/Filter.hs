{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TupleSections         #-}

module Database.DynamoDb.Filter (
      FilterCondition(Not)
    , (&&.), (||.)
    , (==.), (>=.), (>.), (<=.), (<.)
    , attrExists, attrMissing, beginsWith, contains, valIn, between
    , size
    , Column(Column)
    , TypColumn
    , ColName(..)
    , dumpCondition
) where

import           Control.Monad.Supply       (evalSupply, supply)
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.Monoid                ((<>))
import qualified Data.Text                  as T
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDb.Types


type NameGen = T.Text -> (T.Text, ColName)
nameGen :: Column tbl typ ctyp -> NameGen
nameGen (Column name) subst = ("#" <> subst, name)
nameGen (Size name) subst = ("size(#" <> subst <> ")", name)

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
      ident <- supply
      let (subst, ColName colname) = name ident
          expr = subst <> " " <> oper <> " :" <> ident
      return (expr, HMap.singleton ident colname, HMap.singleton ident val)
    go (Between name v1 v2) = do
      idname <- supply
      idstart <- supply
      idstop <- supply
      let (subst, ColName colname) = name idname
          expr = subst <> " BETWEEN :" <> idstart <> " AND :" <> idstop
          vals = HMap.fromList [(idstart, v1), (idstop, v2)]
      return (expr, HMap.singleton idname colname, vals)

    go (In name lst) = do
        idname <- supply
        let (subst, ColName colname) = name idname
        vlist <- mapM (\val -> (,val) <$> supply) lst
        let expr = T.intercalate "," $ map ((":" <>) . fst) vlist
        return (subst <> "IN (" <> expr <> ")", HMap.singleton idname colname, HMap.fromList vlist)

    go (AttrExists name) = do
      ident <- supply
      let (subst, ColName colname) = name ident
          expr = "attribute_exists(" <> subst <> ")"
      return (expr, HMap.singleton ident colname, HMap.empty)
    go (AttrMissing name) = do
      ident <- supply
      let (subst, ColName colname) = name ident
          expr = "attribute_not_exists(" <> subst <> ")"
      return (expr, HMap.singleton ident colname, HMap.empty)
    go (BeginsWith name val) = do
      ident <- supply
      let (subst, ColName colname) = name ident
          expr = "begins_with(" <> subst <> ", :" <> ident <> ")"
      return (expr, HMap.singleton ident colname, HMap.singleton ident val)
    go (Contains name val) = do
      ident <- supply
      let (subst, ColName colname) = name ident
          expr = "contains(" <> subst <> ", :" <> ident <> ")"
      return (expr, HMap.singleton ident colname, HMap.singleton ident val)

between :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> typ -> FilterCondition tbl
between col a b = Between (nameGen col) (dEncode a) (dEncode b)

valIn :: DynamoEncodable typ => Column tbl typ ctyp -> [typ] -> FilterCondition tbl
valIn col lst = In (nameGen col) (map dEncode lst)

attrExists :: Column tbl typ TypColumn -> FilterCondition tbl
attrExists col = AttrExists (nameGen col)

attrMissing :: Column tbl typ TypColumn -> FilterCondition tbl
attrMissing col = AttrMissing (nameGen col)

beginsWith :: IsText typ => Column tbl typ TypColumn -> T.Text -> FilterCondition tbl
beginsWith col txt = BeginsWith (nameGen col) (dEncode txt)

contains :: IsText typ => Column tbl typ TypColumn -> T.Text -> FilterCondition tbl
contains col txt = Contains (nameGen col) (dEncode txt)

size :: Column tbl typ TypColumn -> Column tbl Int TypSize
size (Column colname) = Size colname

dcomp :: DynamoEncodable typ => T.Text -> Column tbl typ ctyp -> typ -> FilterCondition tbl
dcomp op col val = Comparison (nameGen col) op (dEncode val)

(&&.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(&&.) = And

(||.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(||.) = Or

(==.) :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> FilterCondition tbl
(==.) = dcomp "="

(<=.) :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> FilterCondition tbl
(<=.) = dcomp "<="

(<.) :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> FilterCondition tbl
(<.) = dcomp "<"

(>.) :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> FilterCondition tbl
(>.) = dcomp ">"

(>=.) :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> FilterCondition tbl
(>=.) = dcomp ">="
