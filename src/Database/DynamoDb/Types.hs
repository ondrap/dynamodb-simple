{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}

module Database.DynamoDb.Types (
    DynamoEncodable(..)
  , DynamoScalar(..)
  , RangeOper(..)
  , rangeOper
  , rangeData
  , IsText
  , gdEncode
  , gdDecode
  , translateFieldName
) where

import           Control.Lens                ((.~), (^.))
import qualified Data.ByteString             as BS
import           Data.Double.Conversion.Text (toShortest)
import           Data.Function               ((&))
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Monoid                 ((<>))
import           Data.Proxy
import qualified Data.Set                    as Set
import qualified Data.Text                   as T
import           Network.AWS.DynamoDB.Types  (AttributeValue,
                                              ScalarAttributeType,
                                              attributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)
import           Generics.SOP


class DynamoEncodable a => DynamoScalar a where
  dType :: Proxy a -> ScalarAttributeType
instance DynamoScalar Int where
  dType _ = D.N
instance DynamoScalar T.Text where
  dType _ = D.S
instance DynamoScalar BS.ByteString where
  dType _ = D.B

class DynamoEncodable a where
  dEncode :: a -> AttributeValue
  dDecode :: Maybe AttributeValue -> Maybe a

instance DynamoEncodable Integer where
  dEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int where
  dEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Double where
  dEncode num = attributeValue & D.avN .~ (Just $ toShortest num)
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Bool where
  dEncode b = attributeValue & D.avBOOL .~ Just b
  dDecode (Just attr) = attr ^. D.avBOOL
  dDecode Nothing = Nothing
instance DynamoEncodable T.Text where
  dEncode "" = attributeValue -- Empty string is not supported
  dEncode t = attributeValue & D.avS .~ Just t
  dDecode (Just attr) = attr ^. D.avS
  dDecode Nothing = Just ""
instance DynamoEncodable BS.ByteString where
  dEncode t = attributeValue & D.avB .~ Just t
  dDecode Nothing = Nothing
  dDecode (Just attr) = attr ^. D.avB
instance DynamoEncodable a => DynamoEncodable (Maybe a) where
  dEncode Nothing = attributeValue -- ??? Should we set to NULL?
  dEncode (Just key) = dEncode key
  dDecode Nothing = Just Nothing
  dDecode (Just attr) = Just <$> dDecode (Just attr) -- Fail on decoding error, otherwise wrap in Just
instance DynamoEncodable (Set.Set T.Text) where
  dEncode dta = attributeValue & D.avSS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avSS)
  dDecode Nothing = Nothing
instance DynamoEncodable (Set.Set BS.ByteString) where
  dEncode dta = attributeValue & D.avBS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avBS)
  dDecode Nothing = Nothing
instance DynamoEncodable (Set.Set Int) where
  dEncode dta = attributeValue & D.avNS .~ map (T.pack . show) (Set.toList dta)
  dDecode Nothing = Nothing
  dDecode (Just attr) = Set.fromList <$> traverse (readMaybe . T.unpack) (attr ^. D.avNS)
instance DynamoEncodable a => DynamoEncodable (HashMap T.Text a) where
  dEncode dta = attributeValue & D.avM .~ fmap dEncode dta
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avM)
  dDecode Nothing = Nothing
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = attributeValue & D.avL .~ map dEncode lst
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avL)
  dDecode Nothing = Nothing

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
        K $ hcollapse
          $ hcliftA2 pdynamo (\(FieldInfo name) (I val) -> K (T.pack name, dEncode val)) ns xs
    gdEncodeRec _ _ = error "Cannot serialize non-record types."

    palldynamo :: Proxy (All DynamoEncodable)
    palldynamo = Proxy

    pdynamo :: Proxy DynamoEncodable
    pdynamo = Proxy

gdDecode ::
    forall a xs. (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a), Code a ~ '[ xs ])
  => HMap.HashMap T.Text AttributeValue -> Maybe a
gdDecode attrs =
    to . SOP . Z <$> hsequence (hcliftA dproxy decodeAttr (gdFieldNamesNP (Proxy :: Proxy a)))
  where
    decodeAttr :: DynamoEncodable b => K T.Text b -> Maybe b
    decodeAttr (K name) = dDecode (HMap.lookup name attrs)
    dproxy = Proxy :: Proxy DynamoEncodable

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

-- | Function that translates haskell field names to database field names
translateFieldName :: String -> T.Text
translateFieldName = T.pack . translate
  where
    translate ('_':rest) = rest
    translate name
      | '_' `elem` name = drop 1 $ dropWhile (/= '_') name
      | otherwise = name



-- | Class to limit certain operations
class IsNotNumber a
instance IsNotNumber T.Text
instance IsNotNumber BS.ByteString

-- | Class to limit certain operations to text-like only
class IsText a
instance IsText T.Text

data RangeOper a where
  RangeEquals :: a -> RangeOper a
  RangeLessThan :: a -> RangeOper a
  RangeLessThanE :: a -> RangeOper a
  RangeGreaterThan :: a -> RangeOper a
  RangeGreaterThanE :: a -> RangeOper a
  RangeBetween :: a -> a -> RangeOper a
  RangeBeginsWith :: (IsNotNumber a) => a -> RangeOper a

rangeKey :: T.Text
rangeKey = ":rangekey"

rangeStart :: T.Text
rangeStart = ":rangeStart"

rangeEnd :: T.Text
rangeEnd = ":rangeEnd"

rangeOper :: RangeOper a -> T.Text -> T.Text
rangeOper (RangeEquals _) n = "#" <> n <> " = " <> rangeKey
rangeOper (RangeLessThan _) n = "#" <> n <> " < " <> rangeKey
rangeOper (RangeLessThanE _) n = "#" <> n <> " <= " <> rangeKey
rangeOper (RangeGreaterThan _) n = "#" <> n <> " > " <> rangeKey
rangeOper (RangeGreaterThanE _) n = "#" <> n <> " >= " <> rangeKey
rangeOper (RangeBetween _ _) n = "#" <> n <> " BETWEEN " <> rangeStart <> " AND " <> rangeEnd
rangeOper (RangeBeginsWith _) n = "begins_with(#" <> n <> ", " <> rangeKey <> ")"

rangeData :: DynamoScalar a => RangeOper a -> [(T.Text, AttributeValue)]
rangeData (RangeEquals a) = [(rangeKey, dEncode a)]
rangeData (RangeLessThan a) = [(rangeKey, dEncode a)]
rangeData (RangeLessThanE a) = [(rangeKey, dEncode a)]
rangeData (RangeGreaterThan a) = [(rangeKey, dEncode a)]
rangeData (RangeGreaterThanE a) = [(rangeKey, dEncode a)]
rangeData (RangeBetween s e) = [(rangeStart, dEncode s), (rangeEnd, dEncode e)]
rangeData (RangeBeginsWith a) = [(rangeKey, dEncode a)]
