{-# LANGUAGE CPP                   #-}
#if __GLASGOW_HASKELL__ >= 800
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
#endif
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.DynamoDB.Internal where

import           Control.Lens               (Iso', iso)
import           Control.Monad.Supply       (Supply, evalSupply, supply)
import           Data.Foldable              (foldlM)
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.List.NonEmpty         (NonEmpty(..))
import           Data.Semigroup             ((<>))
import           Data.Proxy
import qualified Data.Text                  as T
import           Network.AWS.DynamoDB.Types (AttributeValue)
import qualified Network.AWS.DynamoDB.Types as D
import qualified Data.Semigroup as SEMI

import           Database.DynamoDB.Types

data ColumnType = TypColumn | TypSize

-- | Representation of a column for filter queries
--
-- - typ - datatype of column (Int, Text..)
-- - coltype - TypColumn or TypSize (result of size(column))
-- - col - instance of ColumnInfo, uniquely identify a column
data Column typ (coltype :: ColumnType) col where
    Column :: NonEmpty IntraColName -> Column typ 'TypColumn col
    Size :: NonEmpty IntraColName -> Column Int 'TypSize col

-- | Smart constructor for Column datatype
mkColumn :: forall typ col. ColumnInfo col => Column typ 'TypColumn col
mkColumn = Column (IntraName (columnName (Proxy :: Proxy col)) :| [])

-- | Internal representation of a part of path in a nested structure
data IntraColName = IntraName T.Text | IntraIndex Int

-- Type of query for InCollection (we cannot query on primary key)
data PathType = NestedPath | FullPath

-- | Signifies that the column is present in the table/index
class ColumnInfo col => InCollection col tbl (query :: PathType)

-- | Class to get a column name from a Type specifying a column
class ColumnInfo a where
  columnName :: Proxy a -> T.Text

type NameGen = Supply T.Text T.Text -> Supply T.Text (T.Text, HashMap T.Text T.Text)
nameGen :: Column typ ctyp col -> NameGen
nameGen (Column lst) mkident = nameGenPath lst mkident
nameGen (Size lst) mkident = do
    (path, attrs) <- nameGenPath lst mkident
    return ("size(" Data.Semigroup.<> path <> ")", attrs)

nameGenPath :: NonEmpty IntraColName -> Supply T.Text T.Text -> Supply T.Text (T.Text, HashMap T.Text T.Text)
nameGenPath lst mkident = foldlM joinParts ("", HMap.empty) lst
  where
    joinParts ("", attrs) (IntraName nm) = do
        ident <- mkident
        return (ident, attrs <> HMap.singleton ident nm)
    joinParts (expr, attrs) (IntraName nm) = do
        ident <- mkident
        return (expr <> "." <> ident, attrs <> HMap.singleton ident nm)
    joinParts (expr, attrs) (IntraIndex idx) = return (expr <> "[" <> T.pack (show idx) <> "]", attrs)

-- | Filter condition. Use with scan, query, update and delete methods.
--
-- Filtering on primary key is not allowed.
data FilterCondition t =
      And (FilterCondition t) (FilterCondition t)
    | Or (FilterCondition t) (FilterCondition t)
    | Not (FilterCondition t) -- ^ Negate condition
    | Comparison NameGen T.Text D.AttributeValue
    | AttrExists NameGen
    | AttrMissing NameGen
    | BeginsWith NameGen D.AttributeValue
    | Contains NameGen D.AttributeValue
    | Between NameGen D.AttributeValue D.AttributeValue
    | In NameGen [D.AttributeValue]

instance SEMI.Semigroup (FilterCondition t) where
  (<>) = And

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
rangeOper (RangeEquals _) n = n <> " = " <> rangeKey
rangeOper (RangeLessThan _) n = n <> " < " <> rangeKey
rangeOper (RangeLessThanE _) n = n <> " <= " <> rangeKey
rangeOper (RangeGreaterThan _) n = n <> " > " <> rangeKey
rangeOper (RangeGreaterThanE _) n = n <> " >= " <> rangeKey
rangeOper (RangeBetween _ _) n = n <> " BETWEEN " <> rangeStart <> " AND " <> rangeEnd
rangeOper (RangeBeginsWith _) n = "begins_with(" <> n <> ", " <> rangeKey <> ")"

rangeData :: DynamoScalar v a => RangeOper a -> [(T.Text, AttributeValue)]
rangeData (RangeEquals a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeBetween s e) = [(rangeStart, dScalarEncode s), (rangeEnd, dScalarEncode e)]
rangeData (RangeBeginsWith a) = [(rangeKey, dScalarEncode a)]

-- | Parameter for queries involving read consistency settings.
data Consistency = Eventually | Strongly
  deriving (Show)

-- | Lens to help set consistency.
consistencyL :: Iso' (Maybe Bool) Consistency
consistencyL = iso tocons fromcons
  where
    tocons (Just True) = Strongly
    tocons _ = Eventually
    fromcons Strongly = Just True
    fromcons Eventually = Just False

-- | Query direction
data Direction = Forward | Backward
  deriving (Show, Eq)


-- | Allow skipping over maybe types when using <.>
type family UnMaybe a :: * where
  UnMaybe (Maybe a) = a
  UnMaybe a = a

-- | Combine attributes from nested structures.
--
-- > address' <.> street'
(<.>) :: (InCollection col2 (UnMaybe typ) 'NestedPath)
      => Column typ 'TypColumn col1 -> Column typ2 'TypColumn col2 -> Column typ2 'TypColumn col1
(<.>) (Column a1) (Column a2) = Column (a1 <> a2)
-- It doesn't matter if it is inifxl or infixr; obviously this can be Semigroup instance,
-- but currently as semigroup is not a superclass of monoid, it is probably better to have
-- our own operator.
infixl 7 <.>

-- | Access an index in a nested list.
--
-- > users' <!> 0 <.> name'
(<!>) :: Column [typ] 'TypColumn col -> Int -> Column typ 'TypColumn col
(<!>) (Column a1) num = Column (a1 <> pure (IntraIndex num))
infixl 8 <!>

-- | Access a key in a nested hashmap.
--
-- > phones' <!:> "mobile" <.> number'
(<!:>) :: IsText key => Column (HashMap key typ) 'TypColumn col -> key -> Column typ 'TypColumn col
(<!:>) (Column a1) key = Column (a1 <> pure (IntraName (toText key)))
infixl 8 <!:>
