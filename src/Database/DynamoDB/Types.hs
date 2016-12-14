{-# LANGUAGE CPP                    #-}
#if __GLASGOW_HASKELL__ >= 800
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
#endif
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE PatternSynonyms        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# LANGUAGE ViewPatterns           #-}

-- |
module Database.DynamoDB.Types (
    -- * Exceptions
    DynamoException(..)
    -- * Marshalling
  , DynamoEncodable(..)
  , DynamoScalar(..)
  , ScalarValue(..)
  , IsText(..), IsNumber
    -- * Query datatype
  , RangeOper(..)
    -- * Utility functions
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
import           Data.Maybe                  (mapMaybe)
import           Data.Proxy
import           Data.Scientific             (Scientific, floatingOrInteger,
                                              fromFloatDigits, toBoundedInteger,
                                              toRealFloat)
import qualified Data.Set                    as Set
import           Data.Tagged                 (Tagged (..), unTagged)
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import qualified Data.Vector                 as V
import           Network.AWS.DynamoDB.Types  (AttributeValue,
                                              ScalarAttributeType,
                                              attributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)
import Data.Int (Int16, Int32, Int64)


-- | Exceptions thrown by some dynamodb-simple actions.
data DynamoException = DynamoException T.Text
  deriving (Show)
instance Exception DynamoException

-- | Datatype for encoding scalar values
data ScalarValue (v :: D.ScalarAttributeType) where
    ScS :: !T.Text -> ScalarValue 'D.S
    ScN :: !Scientific -> ScalarValue 'D.N
    ScB :: !BS.ByteString -> ScalarValue 'D.B

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
  dSetEncodeV lst = attributeValue & D.avNS .~ map (\(ScN num) -> decodeUtf8 (toStrict $ AE.encode num)) lst
  dSetDecodeV attr = traverse (\n -> ScN <$> AE.decodeStrict (encodeUtf8 n)) (attr ^. D.avSS)
instance ScalarAuto 'D.B where
  dTypeV _ = D.B
  dSetEncodeV lst = attributeValue & D.avBS .~ map (\(ScB txt) -> txt) lst
  dSetDecodeV attr = Just $ map ScB $ attr ^. D.avBS

dType :: forall a v. DynamoScalar v a => Proxy a -> ScalarAttributeType
dType _ = dTypeV (Proxy :: Proxy v)

dScalarEncode :: DynamoScalar v a => a -> AttributeValue
dScalarEncode a =
  case scalarEncode a of
    ScS txt -> attributeValue & D.avS .~ Just txt
    ScN num -> attributeValue & D.avN .~ Just (decodeUtf8 (toStrict $ AE.encode num))
    ScB bs -> attributeValue & D.avB .~ Just bs

dSetEncode :: DynamoScalar v a => Set.Set a -> AttributeValue
dSetEncode vset = dSetEncodeV $ map scalarEncode $ toList vset

dSetDecode :: (Ord a, DynamoScalar v a) => AttributeValue -> Maybe (Set.Set a)
dSetDecode attr = dSetDecodeV attr >>= traverse scalarDecode >>= pure . Set.fromList

-- | Typeclass signifying that this is a scalar attribute and can be used as a hash/sort key.
--
-- > instance DynamoScalar Network.AWS.DynamoDB.Types.S T.Text where
-- >    scalarEncode = ScS
-- >    scalarDecode (ScS txt) = Just txt
class ScalarAuto v => DynamoScalar (v :: D.ScalarAttributeType) a | a -> v where
  -- | Scalars must have total encoding function
  scalarEncode :: a -> ScalarValue v
  default scalarEncode :: (Show a, Read a) => a -> ScalarValue 'D.S
  scalarEncode = ScS . T.pack . show

  scalarDecode :: ScalarValue v -> Maybe a
  default scalarDecode :: (Show a, Read a) => ScalarValue 'D.S -> Maybe a
  scalarDecode (ScS txt) = readMaybe (T.unpack txt)

instance DynamoScalar 'D.N Integer where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) =
    case floatingOrInteger num :: Either Double Integer of
        Right x -> Just x
        Left _  -> Nothing

instance DynamoScalar 'D.N Int where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'D.N Int16 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'D.N Int32 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'D.N Int64 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num

instance DynamoScalar 'D.N Word where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num

-- | Helper for tagged values
instance {-# OVERLAPPABLE #-} DynamoScalar v a => DynamoScalar v (Tagged x a) where
  scalarEncode = scalarEncode . unTagged
  scalarDecode a = Tagged <$> scalarDecode a

-- | Double as a primary key isn't generally a good thing as equality on double
-- is sometimes a little dodgy. Use scientific instead.
instance DynamoScalar 'D.N Scientific where
  scalarEncode = ScN
  scalarDecode (ScN num) = Just num

-- | Don't use Double as a part of primary key in a table. It is included here
-- for convenience to be used as a range key in indexes.
instance DynamoScalar 'D.N Double where
  scalarEncode = ScN . fromFloatDigits
  scalarDecode (ScN num) = Just $ toRealFloat num

instance DynamoScalar 'D.S T.Text where
  scalarEncode = ScS
  scalarDecode (ScS txt) = Just txt

instance DynamoScalar 'D.B BS.ByteString where
  scalarEncode = ScB
  scalarDecode (ScB bs) = Just bs

-- | Typeclass showing that this datatype can be saved to DynamoDB.
class DynamoEncodable a where
  -- | Encode data. Return 'Nothing' if attribute should be omitted.
  dEncode :: a -> Maybe AttributeValue
  default dEncode :: (Show a, Read a) => a -> Maybe AttributeValue
  dEncode val = Just $ attributeValue & D.avS .~ (Just $ T.pack $ show val)
  -- | Decode data. Return 'Nothing' on parsing error, gets
  --  'Nothing' on input if the attribute was missing in the database.
  dDecode :: Maybe AttributeValue -> Maybe a
  default dDecode :: (Show a, Read a) => Maybe AttributeValue -> Maybe a
  dDecode (Just attr) = attr ^. D.avS >>= (readMaybe . T.unpack)
  dDecode Nothing = Nothing
  -- | Aid for searching for empty list and hashmap; these can be represented
  -- both by empty list and by missing value, if this returns true, enhance search.
  -- Also used by joins to weed out empty foreign keys
  dIsMissing :: a -> Bool
  dIsMissing _ = False

instance DynamoEncodable Scientific where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Integer where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Word where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int16 where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int32 where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int64 where
  dEncode = Just . dScalarEncode
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Double where
  dEncode num = Just $ attributeValue & D.avN .~ (Just $ toShortest num)
  dDecode (Just attr) = attr ^. D.avN >>= AE.decodeStrict . encodeUtf8
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Bool where
  dEncode b = Just $ attributeValue & D.avBOOL .~ Just b
  dDecode (Just attr) = attr ^. D.avBOOL
  dDecode Nothing = Nothing
instance DynamoEncodable T.Text where
  dEncode "" = Nothing
  dEncode txt = Just (dScalarEncode txt)
  dDecode (Just attr)
    | Just True <- attr ^. D.avNULL = Just ""
    | otherwise = attr ^. D.avS
  dDecode Nothing = Just ""
  dIsMissing "" = True
  dIsMissing _ = False
instance DynamoEncodable BS.ByteString where
  dEncode "" = Nothing
  dEncode bs = Just (dScalarEncode bs)
  dDecode (Just attr) = attr ^. D.avB
  dDecode Nothing = Just ""
  dIsMissing "" = True
  dIsMissing _ = False

-- | 'Maybe' ('Maybe' a) will not work well; it will 'join' the value in the database.
instance DynamoEncodable a => DynamoEncodable (Maybe a) where
  dEncode Nothing = Nothing
  dEncode (Just key) = dEncode key
  dDecode Nothing = Just Nothing
  dDecode (Just attr) = Just <$> dDecode (Just attr)
  dIsMissing Nothing = True
  dIsMissing _ = False
instance (Ord a, DynamoScalar v a) => DynamoEncodable (Set.Set a) where
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
-- | DynamoDB cannot represent empty items; ['Maybe' a] will lose Nothings.
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = Just $ attributeValue & D.avL .~ mapMaybe dEncode lst
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avL)
  dDecode Nothing = Just mempty
  dIsMissing = null

instance {-# OVERLAPPABLE #-} DynamoEncodable a => DynamoEncodable (Tagged v a) where
  dEncode = dEncode . unTagged
  dDecode a = Tagged <$> dDecode a
  dIsMissing = dIsMissing . unTagged

-- | Partial encoding/decoding Aeson values. Empty strings get converted to NULL.
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
          [("N", AE.String num)] -> AE.decodeStrict (encodeUtf8 num)
          [("N", num@(AE.Number _))] -> Just num -- Just in case, this is usually not returned
          [("S", AE.String val)] -> Just (AE.String val)
          [("NULL", _)] -> Just AE.Null
          _ -> Nothing
      _ -> Nothing -- This shouldn't happen
  --
  dIsMissing AE.Null = True
  dIsMissing _ = False

-- | Class to limit +=. and -=. for updates.
class IsNumber a
instance IsNumber Int
instance IsNumber Double
instance IsNumber Integer
instance IsNumber a => IsNumber (Tagged t a)

-- | Class to limit certain operations to text-like only in queries.
-- Members of this class can be keys to 'HashMap'.
class (Eq a, Hashable a) => IsText a where
  toText :: a -> T.Text
  fromText :: T.Text -> a
instance IsText T.Text where
  toText = id
  fromText = id
instance (Hashable (Tagged t a), IsText a) => IsText (Tagged t a) where
  toText (Tagged txt) = toText txt
  fromText tg = Tagged (fromText tg)

-- | Operation on range key for 'Database.DynamoDB.query'.
data RangeOper a where
  RangeEquals :: a -> RangeOper a
  RangeLessThan :: a -> RangeOper a
  RangeLessThanE :: a -> RangeOper a
  RangeGreaterThan :: a -> RangeOper a
  RangeGreaterThanE :: a -> RangeOper a
  RangeBetween :: a -> a -> RangeOper a
  RangeBeginsWith :: (IsText a) => a -> RangeOper a
