{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Module for creating filter conditions
--
-- Example as used in nested structure for scan:
--
-- > scanCond Eventually (colCtvrty <!:> "a" <.> colInTreti <.> colInnPrvni ==. "x")
module Database.DynamoDb.Filter (
      -- * Condition datatype
      FilterCondition(Not)
      -- * Logical operators
    , (&&.), (||.)
      -- * Equality comparisons
    , (==.), (/=.), (>=.), (>.), (<=.), (<.)
      -- * Extended functions
    , attrExists, attrMissing, beginsWith, contains, setContains, valIn, between
    , size
) where

import           Control.Lens               ((.~), (^.))
import           Data.Function              ((&))
import           Data.Maybe                 (fromMaybe)
import qualified Data.Set                   as Set
import qualified Data.Text                  as T
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDb.Internal
import           Database.DynamoDb.Types

-- | Numeric/string range comparison
between :: (Ord typ, InCollection col tbl 'FullPath, DynamoScalar typ)
  => Column typ ctyp col -> typ -> typ -> FilterCondition tbl
between col a b = Between (nameGen col) (dScalarEncode a) (dScalarEncode b)

-- | a IN (b, c, d); the list may contain up to 100 values
valIn :: (InCollection col tbl 'FullPath, DynamoScalar typ)
  => Column typ ctyp col -> [typ] -> FilterCondition tbl
valIn col lst = In (nameGen col) (map dScalarEncode lst)

-- | Check existence of attribute
attrExists :: (InCollection col tbl 'FullPath) => Column typ 'TypColumn col -> FilterCondition tbl
attrExists col = AttrExists (nameGen col)

-- | Checks non-existence of an attribute
attrMissing :: (InCollection col tbl 'FullPath) => Column typ 'TypColumn col -> FilterCondition tbl
attrMissing col = AttrMissing (nameGen col)

-- | Comparison for text columns
beginsWith :: (InCollection col tbl 'FullPath, IsText typ)
  => Column typ 'TypColumn col -> T.Text -> FilterCondition tbl
beginsWith col txt = BeginsWith (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for text-like attributes.
contains :: (InCollection col tbl 'FullPath, IsText typ)
  => Column typ 'TypColumn col -> T.Text -> FilterCondition tbl
contains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for sets.
setContains :: (InCollection col tbl 'FullPath, DynamoScalar a)
  => Column (Set.Set a) 'TypColumn col -> a -> FilterCondition tbl
setContains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | Size (i.e. number of bytes) of saved attribute
size :: forall typ col. ColumnInfo col => Column typ 'TypColumn col -> Column Int 'TypSize col
size (Column lst) = Size lst

dcomp :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
  => T.Text -> Column typ ctyp col -> typ -> FilterCondition tbl
dcomp op col val = Comparison (nameGen col) op encval
  where
    -- Ord comparing against nothing doesn't make much sense - failback to NULL
    encval = fromMaybe (D.attributeValue & D.avNULL .~ Just True) (dEncode val)

-- | AND for combining conditions
(&&.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(&&.) = And
infixr 3 &&.

-- | OR for combining conditions
(||.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(||.) = Or
infixr 3 ||.

-- | Tests for equality. Automatically adjusts query to account for missing attributes.
(==.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
  => Column typ ctyp col -> typ -> FilterCondition tbl
(==.) col val =
  case dEncode val of
    -- Hack to have '==. Nothing' correctly working
    Nothing -> AttrMissing (nameGen col)
    -- Hack for '==. ""' or empty set to work correctly on non-initialized values
    Just encval | encval ^. D.avNULL == Just True ->
                          AttrMissing (nameGen col) ||. Comparison (nameGen col) "=" encval
                | otherwise -> Comparison (nameGen col) "=" encval
infix 4 ==.

-- | > a /= b === Not (a == b)
(/=.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(/=.) col val = Not (dcomp "=" col val)
infix 4 /=.


(<=.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(<=.) = dcomp "<="
infix 4 <=.

(<.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(<.) = dcomp "<"
infix 4 <.

(>.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(>.) = dcomp ">"
infix 4 >.

(>=.) :: (InCollection col tbl 'FullPath, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(>=.) = dcomp ">="
infix 4 >=.
