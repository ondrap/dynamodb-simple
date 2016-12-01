{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}

-- |
module Database.DynamoDb.Types (
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
) where

import           Control.Exception           (Exception)
import           Control.Lens                ((.~), (^.))
import qualified Data.ByteString             as BS
import           Data.Double.Conversion.Text (toShortest)
import           Data.Function               ((&))
import           Data.Hashable               (Hashable)
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Maybe                  (catMaybes, mapMaybe)
import           Data.Proxy
import qualified Data.Set                    as Set
import qualified Data.Text                   as T
import           Generics.SOP
import           Network.AWS.DynamoDB.Types  (AttributeValue,
                                              ScalarAttributeType,
                                              attributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)


-- | Exceptions thrown by some dynamodb-simple actions.
data DynamoException = DynamoException T.Text
  deriving (Show)
instance Exception DynamoException

-- | Typeclass signifying that this is a scalar attribute and can be used as a hash/sort key.
class DynamoEncodable a => DynamoScalar a where
  dType :: Proxy a -> ScalarAttributeType
  dScalarEncode :: a -> AttributeValue
instance DynamoScalar Integer where
  dType _ = D.N
  dScalarEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
instance DynamoScalar Int where
  dType _ = D.N
  dScalarEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
instance DynamoScalar T.Text where
  dType _ = D.S
  dScalarEncode "" = attributeValue & D.avNULL .~ Just True-- Empty string is not supported, use null
  dScalarEncode t = attributeValue & D.avS .~ Just t
instance DynamoScalar BS.ByteString where
  dType _ = D.B
  dScalarEncode "" = attributeValue & D.avNULL .~ Just True
  dScalarEncode t = attributeValue & D.avB .~ Just t

-- | Helper pattern
pattern EmptySet <- (Set.null -> True)

-- | Typeclass showing that this datatype can be saved to DynamoDB.
class DynamoEncodable a where
  -- | Encode data. Return 'Nothing' if attribute should be omitted.
  dEncode :: a -> Maybe AttributeValue
  -- | Decode data. Return 'Nothing' on parsing error, gets
  --  'Nothing' on input if the attribute was missing in the database.
  dDecode :: Maybe AttributeValue -> Maybe a

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
instance DynamoEncodable (Set.Set T.Text) where
  dEncode EmptySet = Just $ attributeValue & D.avNULL .~ Just True
  dEncode dta = Just $ attributeValue & D.avSS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avSS)
  dDecode Nothing = Just mempty
instance DynamoEncodable (Set.Set BS.ByteString) where
  dEncode EmptySet = Just $ attributeValue & D.avNULL .~ Just True
  dEncode dta = Just $ attributeValue & D.avBS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avBS)
  dDecode Nothing = Just mempty
instance DynamoEncodable (Set.Set Int) where
  dEncode EmptySet = Just $ attributeValue & D.avNULL .~ Just True
  dEncode dta = Just $ attributeValue & D.avNS .~ map (T.pack . show) (Set.toList dta)
  dDecode (Just attr) = Set.fromList <$> traverse (readMaybe . T.unpack) (attr ^. D.avNS)
  dDecode Nothing = Just mempty
instance (IsText t, DynamoEncodable a) => DynamoEncodable (HashMap t a) where
  dEncode dta =
      let textmap = HMap.fromList $ mapMaybe (\(key, val) -> (toText key,) <$> dEncode val) $ HMap.toList dta
      in Just $ attributeValue & D.avM .~ textmap
  dDecode (Just attr) =
      let attrlist = traverse (\(key, val) -> (fromText key,) <$> dDecode (Just val)) $ HMap.toList (attr ^. D.avM)
      in HMap.fromList <$> attrlist
  dDecode Nothing = Nothing
-- | DynamoDB cannot represent empty items; ['Maybe' a] will lose Nothings
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = Just $ attributeValue & D.avL .~ mapMaybe dEncode lst
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avL)
  dDecode Nothing = Nothing

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
          _ -> error "Cannot happen - gdFieldNamesNP"
    _ -> error "Cannot even patternmatch because of type error"
  where
    getName :: ConstructorInfo xsd -> NP (K T.Text) xsd
    getName (Record _ fields) = hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

-- | Translates haskell field names to database field names.
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
