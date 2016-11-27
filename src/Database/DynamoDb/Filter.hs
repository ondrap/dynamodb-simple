{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE EmptyDataDecls        #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Database.DynamoDb.Filter (
      FilterCondition((&&.), (||.), Not)
    , (==.), (>=.), (>.), (<=.), (<.)
    , attrExists, attrMissing, beginsWith, contains, valIn, between
    , size
    , Column(Column)
    , TypColumn
    , ColName(..)
) where

import qualified Network.AWS.DynamoDB.Types as D

import           Data.Monoid                ((<>))
import qualified Data.Text                  as T
import           Database.DynamoDb.Types

newtype ColName = ColName T.Text
  deriving (Show)

data TypColumn
data TypSize

data Column tbl typ coltype where
    Column :: ColName -> Column tbl typ TypColumn
    Size :: ColName -> Column tbl Int TypSize

colName :: Column tbl typ ctyp -> ColName
colName (Column name) = name
colName (Size (ColName name)) = ColName ("size(" <> name <> ")")

data FilterCondition t =
      (&&.) (FilterCondition t) (FilterCondition t)
    | (||.) (FilterCondition t) (FilterCondition t)
    | Not (FilterCondition t)
    | Comparison ColName T.Text D.AttributeValue
    | AttrExists ColName
    | AttrMissing ColName
    | BeginsWith ColName T.Text
    | Contains ColName T.Text
    | Between ColName D.AttributeValue D.AttributeValue
    | In ColName [D.AttributeValue]
  deriving (Show)

class Operand tbl typ a
instance DynamoEncodable a => Operand tbl a a

between :: DynamoEncodable typ => Column tbl typ ctyp -> typ -> typ -> FilterCondition tbl
between col a b = Between (colName col) (dEncode a) (dEncode b)

valIn :: DynamoEncodable typ => Column tbl typ ctyp -> [typ] -> FilterCondition tbl
valIn col lst = In (colName col) (map dEncode lst)

attrExists :: Column tbl typ TypColumn -> FilterCondition tbl
attrExists col = AttrExists (colName col)

attrMissing :: Column tbl typ TypColumn -> FilterCondition tbl
attrMissing col = AttrMissing (colName col)

beginsWith :: IsText typ => Column tbl typ TypColumn -> T.Text -> FilterCondition tbl
beginsWith col = BeginsWith (colName col)

contains :: IsText typ => Column tbl typ TypColumn -> T.Text -> FilterCondition tbl
contains col = Contains (colName col)

size :: Column tbl typ TypColumn -> Column tbl Int TypSize
size col = Size (colName col)

dcomp :: DynamoEncodable typ => T.Text -> Column tbl typ ctyp -> typ -> FilterCondition tbl
dcomp op col val = Comparison (colName col) op (dEncode val)

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
