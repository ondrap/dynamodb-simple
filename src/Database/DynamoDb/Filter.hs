{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.DynamoDb.Filter (
      FilterCondition(Not)
    , (&&.), (||.)
    , (==.), (/=.), (>=.), (>.), (<=.), (<.)
    , (<.>), (<!>), (<!:>)
    , attrExists, attrMissing, beginsWith, contains, setContains, valIn, between
    , size
    , Column
) where

import           Control.Lens               ((.~), (^.))
import           Data.Function              ((&))
import           Data.HashMap.Strict        (HashMap)
import           Data.List.NonEmpty         (NonEmpty (..))
import           Data.Maybe                 (fromMaybe)
import qualified Data.Set                   as Set
import qualified Data.Text                  as T
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDb.Internal
import           Database.DynamoDb.Types

(<.>) :: forall typ col1 typ2 col2.
        (InCollection col2 typ 'InnerQuery, ColumnInfo col1, ColumnInfo col2)
      => Column typ 'TypColumn col1 -> Column typ2 'TypColumn col2 -> Column typ2 'TypColumn col1
(<.>) (Column (a1 :| rest1)) (Column (a2 :| rest2)) = Column (a1 :| rest1 ++ (a2 : rest2))
-- We need to associate from the right
infixl 7 <.>

(<!>) :: forall typ col. ColumnInfo col => Column [typ] 'TypColumn col -> Int -> Column typ 'TypColumn col
(<!>) (Column (a1 :| rest)) num = Column (a1 :| (rest ++ [IntraIndex num]))
infixl 8 <!>

(<!:>) :: forall typ col key. (ColumnInfo col, IsText key)
    => Column (HashMap key typ) 'TypColumn col -> T.Text -> Column typ 'TypColumn col
(<!:>) (Column (a1 :| rest)) key = Column (a1 :| (rest ++ [IntraName (toText key)]))
infixl 8 <!:>

between :: (Ord typ, InCollection col tbl 'OuterQuery, DynamoScalar typ)
  => Column typ ctyp col -> typ -> typ -> FilterCondition tbl
between col a b = Between (nameGen col) (dScalarEncode a) (dScalarEncode b)

valIn :: (InCollection col tbl 'OuterQuery, DynamoScalar typ)
  => Column typ ctyp col -> [typ] -> FilterCondition tbl
valIn col lst = In (nameGen col) (map dScalarEncode lst)

attrExists :: (InCollection col tbl 'OuterQuery) => Column typ 'TypColumn col -> FilterCondition tbl
attrExists col = AttrExists (nameGen col)

attrMissing :: (InCollection col tbl 'OuterQuery) => Column typ 'TypColumn col -> FilterCondition tbl
attrMissing col = AttrMissing (nameGen col)

beginsWith :: (InCollection col tbl 'OuterQuery, IsText typ)
  => Column typ 'TypColumn col -> T.Text -> FilterCondition tbl
beginsWith col txt = BeginsWith (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for rext-like attributes
contains :: (InCollection col tbl 'OuterQuery, IsText typ)
  => Column typ 'TypColumn col -> T.Text -> FilterCondition tbl
contains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | CONTAINS condition for sets
setContains :: (InCollection col tbl 'OuterQuery, DynamoScalar a)
  => Column (Set.Set a) 'TypColumn col -> a -> FilterCondition tbl
setContains col txt = Contains (nameGen col) (dScalarEncode txt)

-- | Size (i.e. number of bytes) of saved attribute
size :: forall typ col. ColumnInfo col => Column typ 'TypColumn col -> Column Int 'TypSize col
size (Column lst) = Size lst

dcomp :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ)
  => T.Text -> Column typ ctyp col -> typ -> FilterCondition tbl
dcomp op col val = Comparison (nameGen col) op encval
  where
    -- Ord comparing against nothing doesn't make much sense - failback to NULL
    encval = fromMaybe (D.attributeValue & D.avNULL .~ Just True) (dEncode val)

(&&.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(&&.) = And
infixr 3 &&.

(||.) :: FilterCondition t -> FilterCondition t -> FilterCondition t
(||.) = Or
infixr 3 ||.

(==.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ)
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

(/=.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(/=.) col val = Not (dcomp "=" col val)
infix 4 /=.


(<=.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(<=.) = dcomp "<="
infix 4 <=.

(<.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(<.) = dcomp "<"
infix 4 <.

(>.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(>.) = dcomp ">"
infix 4 >.

(>=.) :: (InCollection col tbl 'OuterQuery, DynamoEncodable typ, Ord typ)
        => Column typ ctyp col -> typ -> FilterCondition tbl
(>=.) = dcomp ">="
infix 4 >=.
