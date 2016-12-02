{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}
{-# LANGUAGE DefaultSignatures   #-}
{-# LANGUAGE KindSignatures   #-}
{-# LANGUAGE MultiParamTypeClasses   #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# LANGUAGE FunctionalDependencies   #-}

-- |
module Database.DynamoDB.Types (
    -- * Exceptions
    DynamoException(..)
    -- * Marshalling
  , DynamoEncodable(..)
  , DynamoScalar(..)
  , IsText(..), IsNumber
    -- * Query datatype
  , RangeOper(..)
    -- * Utility functions
  , gdEncode
  , gdDecode
  , translateFieldName
  , dType
  , dScalarEncode
) where

import           Control.Exception           (Exception)
import           Control.Lens                ((.~), (^.))
import qualified Data.Aeson                  as AE
import qualified Data.ByteString             as BS
import           Data.ByteString.Lazy        (toStrict)
import           Data.Double.Conversion.Text (toShortest)
import           Data.Foldable               (toList)
import           Data.Function               ((&))
import           Data.Hashable               (Hashable)
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Maybe                  (catMaybes, mapMaybe)
import           Data.Proxy
import qualified Data.Set                    as Set
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8)
import qualified Data.Vector                 as V
import           Generics.SOP
import           Network.AWS.DynamoDB.Types  (AttributeValue,
                                              ScalarAttributeType,
                                              attributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)
import Data.Scientific (Scientific, toBoundedInteger, isInteger)


-- | Exceptions thrown by some dynamodb-simple actions.
data DynamoException = DynamoException T.Text
  deriving (Show)
instance Exception DynamoException

data ScalarValue (v :: D.ScalarAttributeType) where
    ScS :: T.Text -> ScalarValue 'D.S
    ScN :: Scientific -> ScalarValue 'D.N
    ScB :: BS.ByteString -> ScalarValue 'D.B

class ScalarAuto (v :: D.ScalarAttributeType) where
  dTypeV :: Proxy v -> ScalarAttributeType
  dSetEncodeV :: [ScalarValue v] -> AttributeValue
  dSetDecodeV :: AttributeValue -> Maybe [ScalarValue v]
instance ScalarAuto 'D.S where
  dTypeV _ = D.S
  dSetEncodeV lst = attributeValue & D.avSS .~ map (\(ScS txt) -> txt) lst
  dSetDecodeV attr = Just $ map ScS $ attr ^. D.avSS
instance ScalarAuto 'D.N where
  dTypeV _ = D.N
  dSetEncodeV lst = attributeValue & D.avNS .~ map (\(ScN num) -> T.pack (show num)) lst
  dSetDecodeV attr = traverse (\n -> ScN <$> readMaybe (T.unpack n)) (attr ^. D.avSS)
instance ScalarAuto 'D.B where
  dTypeV _ = D.B
  dSetEncodeV lst = attributeValue & D.avBS .~ map (\(ScB txt) -> txt) lst
  dSetDecodeV attr = Just $ map ScB $ attr ^. D.avBS

dType :: forall a v. DynamoScalar a v => Proxy a -> ScalarAttributeType
dType _ = dTypeV (Proxy :: Proxy v)

dScalarEncode :: DynamoScalar a v => a -> AttributeValue
dScalarEncode a =
  case scalarEncode a of
    ScS txt -> attributeValue & D.avS .~ Just txt
    ScN num -> attributeValue & D.avN .~ Just (T.pack (show num))
    ScB bs -> attributeValue & D.avB .~ Just bs

dSetEncode :: DynamoScalar a v => Set.Set a -> AttributeValue
dSetEncode vset = dSetEncodeV $ map scalarEncode $ toList vset

dSetDecode :: (Ord a, DynamoScalar a v) => AttributeValue -> Maybe (Set.Set a)
dSetDecode attr = dSetDecodeV attr >>= traverse scalarDecode >>= pure . Set.fromList

-- | Typeclass signifying that this is a scalar attribute and can be used as a hash/sort key.
class (ScalarAuto v, DynamoEncodable a) => DynamoScalar a (v :: D.ScalarAttributeType) | a -> v where
  -- | Scalars must have total encoding function
  scalarEncode :: a -> ScalarValue v
  scalarDecode :: ScalarValue v -> Maybe a

instance DynamoScalar Integer 'D.N where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num)
    | isInteger num = Just $ truncate num
    | otherwise = Nothing
instance DynamoScalar Int 'D.N where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar T.Text 'D.S where
  scalarEncode = ScS
  scalarDecode (ScS txt) = Just txt
instance DynamoScalar BS.ByteString 'D.B where
  scalarEncode = ScB
  scalarDecode (ScB bs) = Just bs

-- | Typeclass showing that this datatype can be saved to DynamoDB.
class DynamoEncodable a where
  -- | Encode data. Return 'Nothing' if attribute should be omitted.
  dEncode :: a -> Maybe AttributeValue
  -- | Decode data. Return 'Nothing' on parsing error, gets
  --  'Nothing' on input if the attribute was missing in the database.
  dDecode :: Maybe AttributeValue -> Maybe a
  -- | Aid for searching for empty list and hashmap; these can be represented
  -- both by empty list and by missing value, if this returns true, enhance search.
  dIsMissing :: a -> Bool
  dIsMissing _ = False

instance DynamoEncodable Integer where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Double where
  dEncode num = Just $ attributeValue & D.avN .~ (Just $ toShortest num)
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Bool where
  dEncode b = Just $ attributeValue & D.avBOOL .~ Just b
  dDecode (Just attr) = attr ^. D.avBOOL
  dDecode Nothing = Nothing
instance DynamoEncodable T.Text where
  dEncode = Just . dScalarEncode
  dDecode (Just attr)
    | Just True <- attr ^. D.avNULL = Just ""
    | otherwise = attr ^. D.avS
  dDecode Nothing = Just ""
instance DynamoEncodable BS.ByteString where
  dEncode = Just . dScalarEncode
  dDecode (Just attr)
    | Just True <- attr ^. D.avNULL = Just ""
    | otherwise = attr ^. D.avB
  dDecode Nothing = Just ""

-- | 'Maybe' ('Maybe' a) will not work well; it will 'join' the value in the database.
instance DynamoEncodable a => DynamoEncodable (Maybe a) where
  dEncode Nothing = Nothing
  dEncode (Just key) = dEncode key
  dDecode Nothing = Just Nothing
  dDecode (Just attr) = Just <$> dDecode (Just attr)
instance (Ord a, DynamoScalar a v) => DynamoEncodable (Set.Set a) where
  dEncode (Set.null -> True) = Nothing
  dEncode dta = Just $ dSetEncode dta
  dDecode (Just attr) = dSetDecode attr
  dDecode Nothing = Just Set.empty
instance (IsText t, DynamoEncodable a) => DynamoEncodable (HashMap t a) where
  dEncode dta =
      let textmap = HMap.fromList $ mapMaybe (\(key, val) -> (toText key,) <$> dEncode val) $ HMap.toList dta
      in Just $ attributeValue & D.avM .~ textmap
  dDecode (Just attr) =
      let attrlist = traverse (\(key, val) -> (fromText key,) <$> dDecode (Just val)) $ HMap.toList (attr ^. D.avM)
      in HMap.fromList <$> attrlist
  dDecode Nothing = Just mempty
  dIsMissing = null
-- | DynamoDB cannot represent empty items; ['Maybe' a] will lose Nothings
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = Just $ attributeValue & D.avL .~ mapMaybe dEncode lst
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avL)
  dDecode Nothing = Just mempty
  dIsMissing = null

-- | Partial encoding/decoding Aeson values. Empty strings get converted to NULL.
-- This is not a raw API type; if a set is encountered, deserialization fails.
instance DynamoEncodable AE.Value where
  dEncode (AE.Object obj) = dEncode obj
  dEncode (AE.Array lst) = dEncode (toList lst)
  dEncode (AE.String txt) = dEncode txt
  dEncode num@(AE.Number _) = Just $ attributeValue & D.avN .~ Just (decodeUtf8 (toStrict $ AE.encode num))
  dEncode (AE.Bool b) = dEncode b
  dEncode AE.Null = Just $ attributeValue & D.avNULL .~ Just True
  --
  dDecode Nothing = Just AE.Null
  dDecode (Just attr) = -- Ok, this is going to be very hacky...
    case AE.toJSON attr of
      AE.Object obj -> case HMap.toList obj of
          [("BOOL", AE.Bool val)] -> Just (AE.Bool val)
          [("L", _)] -> (AE.Array .V.fromList) <$> mapM (dDecode . Just) (attr ^. D.avL)
          [("M", _)] -> AE.Object <$> mapM (dDecode . Just) (attr ^. D.avM)
          [("N", val)] -> Just val
          [("S", AE.String val)] -> Just (AE.String val)
          [("NULL", _)] -> Just AE.Null
          _ -> Nothing
      _ -> Nothing -- This shouldn't happen
  --
  dIsMissing AE.Null = True
  dIsMissing _ = False

-- | Encode a record to hashmap using generic-sop.
gdEncode :: forall a. (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a))
  => a -> HashMap T.Text AttributeValue
gdEncode a =
  HMap.fromList $
    case datatypeInfo (Proxy :: Proxy a) of
      ADT _ _ cs -> gdEncode' cs (from a)
      Newtype _ _ c -> gdEncode' (c :* Nil) (from a)
  where
    gdEncode' :: All2 DynamoEncodable xs => NP ConstructorInfo xs -> SOP I xs -> [(T.Text, AttributeValue)]
    gdEncode' cs (SOP sop) = hcollapse $ hcliftA2 palldynamo gdEncodeRec cs sop

    gdEncodeRec :: All DynamoEncodable xs => ConstructorInfo xs -> NP I xs -> K [(T.Text, AttributeValue)] xs
    gdEncodeRec (Record _ ns) xs =
        K $ catMaybes $ hcollapse
          $ hcliftA2 pdynamo (\(FieldInfo name) (I val) -> K ((T.pack name,) <$> dEncode val)) ns xs
    gdEncodeRec _ _ = error "Cannot serialize non-record types."

    palldynamo :: Proxy (All DynamoEncodable)
    palldynamo = Proxy

    pdynamo :: Proxy DynamoEncodable
    pdynamo = Proxy

-- | Decode hashmap to a record using generic-sop.
gdDecode ::
    forall a xs. (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a), Code a ~ '[ xs ])
  => HMap.HashMap T.Text AttributeValue -> Maybe a
gdDecode attrs =
    to . SOP . Z <$> hsequence (hcliftA dproxy decodeAttr (gdFieldNamesNP (Proxy :: Proxy a)))
  where
    decodeAttr :: DynamoEncodable b => K T.Text b -> Maybe b
    decodeAttr (K name) = dDecode (HMap.lookup name attrs)
    dproxy = Proxy :: Proxy DynamoEncodable

-- | Return record field names in NP structure.
gdFieldNamesNP :: forall a xs. (HasDatatypeInfo a, Code a ~ '[ xs ]) => Proxy a -> NP (K T.Text) xs
gdFieldNamesNP _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs ->
        case hliftA getName cs of
          start :* Nil -> start
    _ -> error "Cannot even patternmatch because of type error"
  where
    getName :: ConstructorInfo xsd -> NP (K T.Text) xsd
    getName (Record _ fields) = hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

-- | Translates haskell field names to database attribute names.
translateFieldName :: String -> T.Text
translateFieldName = T.pack . translate
  where
    translate ('_':rest) = rest
    translate name
      | '_' `elem` name = drop 1 $ dropWhile (/= '_') name
      | otherwise = name

-- | Class to limit certain operations for updates.
class IsNumber a
instance IsNumber Int
instance IsNumber Double
instance IsNumber Integer

-- | Class to limit certain operations to text-like only in queries.
-- Members of this class can be keys to 'HashMap'.
class (Eq a, Hashable a) => IsText a where
  toText :: a -> T.Text
  fromText :: T.Text -> a
instance IsText T.Text where
  toText = id
  fromText = id

-- | Operation on range key for 'Database.queryKey.queryKey'.
data RangeOper a where
  RangeEquals :: a -> RangeOper a
  RangeLessThan :: a -> RangeOper a
  RangeLessThanE :: a -> RangeOper a
  RangeGreaterThan :: a -> RangeOper a
  RangeGreaterThanE :: a -> RangeOper a
  RangeBetween :: a -> a -> RangeOper a
  RangeBeginsWith :: (IsText a) => a -> RangeOper a
