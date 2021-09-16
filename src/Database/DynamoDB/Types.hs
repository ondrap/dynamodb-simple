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
import           Control.Lens                ((.~), (^.), (?~))
import qualified Data.Aeson                  as AE
import           Data.Bifunctor              (first)
import qualified Data.ByteString             as BS
import           Data.ByteString.Lazy        (toStrict)
import           Data.Double.Conversion.Text (toShortest)
import           Data.Foldable               (toList)
import           Data.Function               ((&))
import           Data.Hashable               (Hashable)
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Maybe                  (fromMaybe, mapMaybe)
import           Data.Monoid                 ((<>))
import           Data.Proxy
import           Data.UUID.Types             (UUID)
import qualified Data.UUID.Types             as UUID
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
                                              newAttributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)
import Data.Int (Int16, Int32, Int64)


-- | Exceptions thrown by some dynamodb-simple actions.
data DynamoException = DynamoException T.Text
  deriving (Show)
instance Exception DynamoException

data ScalarValueType
  = ScalarValueType_S
  | ScalarValueType_N
  | ScalarValueType_B

-- | Datatype for encoding scalar values
data ScalarValue (v :: ScalarValueType) where
    ScS :: !T.Text -> ScalarValue 'ScalarValueType_S
    ScN :: !Scientific -> ScalarValue 'ScalarValueType_N
    ScB :: !BS.ByteString -> ScalarValue 'ScalarValueType_B


class ScalarAuto (v :: ScalarValueType) where
  dTypeV :: Proxy v -> ScalarAttributeType
  dSetEncodeV :: [ScalarValue v] -> AttributeValue
  dSetDecodeV :: AttributeValue -> Maybe [ScalarValue v]
instance ScalarAuto 'ScalarValueType_S where
  dTypeV _ = D.ScalarAttributeType_S
  dSetEncodeV lst = newAttributeValue & D.attributeValue_ss ?~ map (\(ScS txt) -> txt) lst
  dSetDecodeV attr = Just $ map ScS $ fromMaybe mempty $ attr ^. D.attributeValue_ss
instance ScalarAuto 'ScalarValueType_N where
  dTypeV _ = D.ScalarAttributeType_N
  dSetEncodeV lst = newAttributeValue & D.attributeValue_ns ?~ map (\(ScN num) -> decodeUtf8 (toStrict $ AE.encode num)) lst
  dSetDecodeV attr = traverse (\n -> ScN <$> AE.decodeStrict (encodeUtf8 n)) (fromMaybe mempty (attr ^. D.attributeValue_ss))
instance ScalarAuto 'ScalarValueType_B where
  dTypeV _ = D.ScalarAttributeType_B
  dSetEncodeV lst = newAttributeValue & D.attributeValue_bs ?~ map (\(ScB txt) -> txt) lst
  dSetDecodeV attr = Just $ map ScB $ fromMaybe mempty $ attr ^. D.attributeValue_bs

dType :: forall a v. DynamoScalar v a => Proxy a -> ScalarAttributeType
dType _ = dTypeV (Proxy :: Proxy v)

dScalarEncode :: DynamoScalar v a => a -> AttributeValue
dScalarEncode a =
  case scalarEncode a of
    ScS txt -> newAttributeValue & D.attributeValue_s .~ Just txt
    ScN num -> newAttributeValue & D.attributeValue_n .~ Just (decodeUtf8 (toStrict $ AE.encode num))
    ScB bs -> newAttributeValue & D.attributeValue_b .~ Just bs

dSetEncode :: DynamoScalar v a => Set.Set a -> AttributeValue
dSetEncode vset = dSetEncodeV $ map scalarEncode $ toList vset

dSetDecode :: (Ord a, DynamoScalar v a) => AttributeValue -> Maybe (Set.Set a)
dSetDecode attr = dSetDecodeV attr >>= traverse scalarDecode >>= pure . Set.fromList

-- | Typeclass signifying that this is a scalar attribute and can be used as a hash/sort key.
--
-- > instance DynamoScalar Network.AWS.DynamoDB.Types.S T.Text where
-- >    scalarEncode = ScS
-- >    scalarDecode (ScS txt) = Just txt
class ScalarAuto v => DynamoScalar (v :: ScalarValueType) a | a -> v where
  -- | Scalars must have total encoding function
  scalarEncode :: a -> ScalarValue v
  default scalarEncode :: (Show a, Read a, v ~ 'ScalarValueType_S) => a -> ScalarValue v
  scalarEncode = ScS . T.pack . show

  scalarDecode :: ScalarValue v -> Maybe a
  default scalarDecode :: (Show a, Read a, v ~ 'ScalarValueType_S) => ScalarValue v -> Maybe a
  scalarDecode (ScS txt) = readMaybe (T.unpack txt)

instance DynamoScalar 'ScalarValueType_N Integer where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) =
    case floatingOrInteger num :: Either Double Integer of
        Right x -> Just x
        Left _  -> Nothing

instance DynamoScalar 'ScalarValueType_N Int where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'ScalarValueType_N Int16 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'ScalarValueType_N Int32 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num
instance DynamoScalar 'ScalarValueType_N Int64 where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num

instance DynamoScalar 'ScalarValueType_N Word where
  scalarEncode = ScN . fromIntegral
  scalarDecode (ScN num) = toBoundedInteger num

-- | Helper for tagged values
instance {-# OVERLAPPABLE #-} DynamoScalar v a => DynamoScalar v (Tagged x a) where
  scalarEncode = scalarEncode . unTagged
  scalarDecode a = Tagged <$> scalarDecode a

-- | Double as a primary key isn't generally a good thing as equality on double
-- is sometimes a little dodgy. Use scientific instead.
instance DynamoScalar 'ScalarValueType_N Scientific where
  scalarEncode = ScN
  scalarDecode (ScN num) = Just num

-- | Don't use Double as a part of primary key in a table. It is included here
-- for convenience to be used as a range key in indexes.
instance DynamoScalar 'ScalarValueType_N Double where
  scalarEncode = ScN . fromFloatDigits
  scalarDecode (ScN num) = Just $ toRealFloat num

instance DynamoScalar 'ScalarValueType_S T.Text where
  scalarEncode = ScS
  scalarDecode (ScS txt) = Just txt

instance DynamoScalar 'ScalarValueType_B BS.ByteString where
  scalarEncode = ScB
  scalarDecode (ScB bs) = Just bs

-- | Typeclass showing that this datatype can be saved to DynamoDB.
class DynamoEncodable a where
  -- | Encode data. Return 'Nothing' if attribute should be omitted.
  dEncode :: a -> Maybe AttributeValue
  default dEncode :: (Show a, Read a) => a -> Maybe AttributeValue
  dEncode val = Just $ newAttributeValue & D.attributeValue_s .~ (Just $ T.pack $ show val)
  -- | Decode data. Return 'Nothing' on parsing error, gets
  --  'Nothing' on input if the attribute was missing in the database.
  dDecode :: Maybe AttributeValue -> Maybe a
  default dDecode :: (Show a, Read a) => Maybe AttributeValue -> Maybe a
  dDecode (Just attr) = attr ^. D.attributeValue_s >>= (readMaybe . T.unpack)
  dDecode Nothing = Nothing

  -- | Decode data. Return (Left err) on parsing error, gets
  --  'Nothing' on input if the attribute was missing in the database.
  -- The default instance uses dDecode, define this just for better errors
  dDecodeEither :: Maybe AttributeValue -> Either T.Text a
  dDecodeEither = maybe (Left "Decoding error") Right . dDecode
 
  -- | Aid for searching for empty list and hashmap; these can be represented
  -- both by empty list and by missing value, if this returns true, enhance search.
  -- Also used by joins to weed out empty foreign keys
  dIsMissing :: a -> Bool
  dIsMissing _ = False

instance DynamoEncodable Scientific where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Integer where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Int where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Word where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Int16 where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Int32 where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Int64 where
  dEncode = Just . dScalarEncode
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Double where
  dEncode num = Just $ newAttributeValue & D.attributeValue_n .~ (Just $ toShortest num)
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = 
      maybe (Left "Missing value") Right (attr ^. D.attributeValue_n)
      >>= first T.pack . AE.eitherDecodeStrict . encodeUtf8
  dDecodeEither Nothing = Left "Missing attr"
instance DynamoEncodable Bool where
  dEncode b = Just $ newAttributeValue & D.attributeValue_bool .~ Just b
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = maybe (Left "Missing value") Right (attr ^. D.attributeValue_bool)
  dDecodeEither Nothing = Left "Missing attr"

instance DynamoEncodable T.Text where
  dEncode "" = Nothing
  dEncode txt = Just (dScalarEncode txt)
  dDecode (Just attr)
    | Just True <- attr ^. D.attributeValue_null = Just ""
    | otherwise = attr ^. D.attributeValue_s
  dDecode Nothing = Just ""
  dIsMissing "" = True
  dIsMissing _ = False
instance DynamoEncodable BS.ByteString where
  dEncode "" = Nothing
  dEncode bs = Just (dScalarEncode bs)
  dDecode (Just attr) = attr ^. D.attributeValue_b
  dDecode Nothing = Just ""
  dIsMissing "" = True
  dIsMissing _ = False


instance DynamoEncodable UUID where
  dEncode uuid = dEncode (UUID.toText uuid)
  dDecode attr = attr >>= dDecode . Just >>= UUID.fromText
instance DynamoScalar 'ScalarValueType_S UUID where
  scalarEncode = ScS . UUID.toText
  scalarDecode (ScS txt) = UUID.fromText txt

-- | 'Maybe' ('Maybe' a) will not work well; it will 'join' the value in the database.
instance DynamoEncodable a => DynamoEncodable (Maybe a) where
  dEncode Nothing = Nothing
  dEncode (Just key) = dEncode key
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither Nothing = Right Nothing
  dDecodeEither (Just attr) = Just <$> dDecodeEither (Just attr)
  dIsMissing Nothing = True
  dIsMissing _ = False
instance (Ord a, DynamoScalar v a) => DynamoEncodable (Set.Set a) where
  dEncode (Set.null -> True) = Nothing
  dEncode dta = Just $ dSetEncode dta
  dDecode (Just attr) = dSetDecode attr
  dDecode Nothing = Just Set.empty
  dDecodeEither (Just attr) = maybe (Left "Error decoding set") Right (dSetDecode attr)
  dDecodeEither Nothing = Right Set.empty
instance (IsText t, DynamoEncodable a) => DynamoEncodable (HashMap t a) where
  dEncode dta =
      let textmap = HMap.fromList $ mapMaybe (\(key, val) -> (toText key,) <$> dEncode val) $ HMap.toList dta
      in Just $ newAttributeValue & D.attributeValue_m ?~ textmap
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) =
    let attrlist = traverse (\(key, val) -> (fromText key,) <$> dDecodeEither (Just val)) $ HMap.toList (fromMaybe mempty (attr ^. D.attributeValue_m))
    in HMap.fromList <$> attrlist
  dDecodeEither Nothing = Right mempty
  dIsMissing = null
-- | DynamoDB cannot represent empty items; ['Maybe' a] will lose Nothings.
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = Just $ newAttributeValue & D.attributeValue_l ?~ mapMaybe dEncode lst
  dDecode = either (const Nothing) Just . dDecodeEither
  dDecodeEither (Just attr) = traverse (dDecodeEither . Just) (fromMaybe mempty (attr ^. D.attributeValue_l))
  dDecodeEither Nothing = Right mempty
  dIsMissing = null

instance {-# OVERLAPPABLE #-} DynamoEncodable a => DynamoEncodable (Tagged v a) where
  dEncode = dEncode . unTagged
  dDecode a = Tagged <$> dDecode a
  dDecodeEither a = Tagged <$> dDecodeEither a
  dIsMissing = dIsMissing . unTagged

-- | Partial encoding/decoding Aeson values. Empty strings get converted to NULL.
instance DynamoEncodable AE.Value where
  dEncode (AE.Object obj) = dEncode obj
  dEncode (AE.Array lst) = dEncode (toList lst)
  dEncode (AE.String txt) = dEncode txt
  dEncode num@(AE.Number _) = Just $ newAttributeValue & D.attributeValue_n .~ Just (decodeUtf8 (toStrict $ AE.encode num))
  dEncode (AE.Bool b) = dEncode b
  dEncode AE.Null = Just $ newAttributeValue & D.attributeValue_null .~ Just True
  --
  dDecode = either (const Nothing) Just . dDecodeEither

  --dDecodeEither :: Maybe AttributeValue -> Either T.Text AE.Object --TODO: drop
  dDecodeEither Nothing = Right AE.Null
  dDecodeEither (Just attr) = -- Ok, this is going to be very hacky...
    case AE.toJSON attr of
      AE.Object obj -> case HMap.toList obj of
          [("BOOL", AE.Bool val)] -> Right (AE.Bool val)
          [("L", _)] -> (AE.Array .V.fromList) <$> mapM (dDecodeEither . Just) (fromMaybe mempty (attr ^. D.attributeValue_l))
          [("M", _)] -> AE.Object <$> mapM (dDecodeEither . Just) (fromMaybe mempty (attr ^. D.attributeValue_m))
          [("N", AE.String num)] -> first T.pack (AE.eitherDecodeStrict (encodeUtf8 num))
          [("N", num@(AE.Number _))] -> Right num -- Just in case, this is usually not returned
          [("S", AE.String val)] -> Right (AE.String val)
          [("NULL", _)] -> Right AE.Null
          _ -> Left ("Undecodable json value: " Data.Monoid.<> decodeUtf8 (toStrict (AE.encode obj)))
      _ -> Left "Wrong dynamo data" -- This shouldn't happen
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
