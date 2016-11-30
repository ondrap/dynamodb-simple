{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}

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
  , DynamoException(..)
  , catMaybes
  , dScalarEncode
) where

import           Control.Exception           (Exception)
import           Control.Lens                ((.~), (^.))
import qualified Data.ByteString             as BS
import           Data.Double.Conversion.Text (toShortest)
import           Data.Function               ((&))
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Maybe                  (catMaybes, mapMaybe, fromMaybe)
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


data DynamoException = DynamoException T.Text
  deriving (Show)
instance Exception DynamoException

class DynamoEncodable a => DynamoScalar a where
  dType :: Proxy a -> ScalarAttributeType
instance DynamoScalar Int where
  dType _ = D.N
instance DynamoScalar T.Text where
  dType _ = D.S
instance DynamoScalar BS.ByteString where
  dType _ = D.B

-- | Helper function
dScalarEncode :: DynamoEncodable a => a -> AttributeValue
dScalarEncode = fromMaybe (error "dEncode return Null value") . dEncode

class DynamoEncodable a where
  -- | Must return Just for scalar values
  dEncode :: a -> Maybe AttributeValue
  dDecode :: Maybe AttributeValue -> Maybe a
  -- | Helper to allow 'colSomething == Nothing' to correctly fall back to attr_missing
  dIsNothing :: a -> Bool
  dIsNothing _ = False

instance DynamoEncodable Integer where
  dEncode num = Just $ attributeValue & D.avN .~ (Just $ T.pack (show num))
  dDecode (Just attr) = attr ^. D.avN >>= readMaybe . T.unpack
  dDecode Nothing = Nothing -- Fail on missing attr
instance DynamoEncodable Int where
  dEncode num = Just $ attributeValue & D.avN .~ (Just $ T.pack (show num))
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
  dEncode "" = Just $ attributeValue & D.avNULL .~ Just True-- Empty string is not supported, use null
  dEncode t = Just $ attributeValue & D.avS .~ Just t
  dDecode (Just attr)
    | Just True <- attr ^. D.avNULL = Just ""
    | otherwise = attr ^. D.avS
  dDecode Nothing = Nothing
instance DynamoEncodable BS.ByteString where
  dEncode "" = Just $ attributeValue & D.avNULL .~ Just True
  dEncode t = Just $ attributeValue & D.avB .~ Just t
  dDecode (Just attr)
    | Just True <- attr ^. D.avNULL = Just ""
    | otherwise = attr ^. D.avB
  dDecode Nothing = Nothing

-- | Maybe (Maybe a) will not work well; it will 'join' the result
instance DynamoEncodable a => DynamoEncodable (Maybe a) where
  dEncode Nothing = Nothing
  dEncode (Just key) = dEncode key
  dDecode Nothing = Just Nothing
  dDecode (Just attr) = Just <$> dDecode (Just attr)
  dIsNothing Nothing = True
  dIsNothing _ = False
instance DynamoEncodable (Set.Set T.Text) where
  dEncode dta = Just $ attributeValue & D.avSS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avSS)
  dDecode Nothing = Nothing
instance DynamoEncodable (Set.Set BS.ByteString) where
  dEncode dta = Just $ attributeValue & D.avBS .~ Set.toList dta
  dDecode (Just attr) = Just $ Set.fromList (attr ^. D.avBS)
  dDecode Nothing = Nothing
instance DynamoEncodable (Set.Set Int) where
  dEncode dta = Just $ attributeValue & D.avNS .~ map (T.pack . show) (Set.toList dta)
  dDecode Nothing = Nothing
  dDecode (Just attr) = Set.fromList <$> traverse (readMaybe . T.unpack) (attr ^. D.avNS)
-- | DynamoDB cannot represent empty items; {key:Maybe a} will lose Nothings
instance DynamoEncodable a => DynamoEncodable (HashMap T.Text a) where
  dEncode dta = Just $ attributeValue & D.avM .~ HMap.mapMaybe dEncode dta
  dDecode (Just attr) = traverse (dDecode . Just) (attr ^. D.avM)
  dDecode Nothing = Nothing
-- | DynamoDB cannot represent empty items; [Maybe a] will lose Nothings
instance DynamoEncodable a => DynamoEncodable [a] where
  dEncode lst = Just $ attributeValue & D.avL .~ mapMaybe dEncode lst
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
        K $ catMaybes $ hcollapse
          $ hcliftA2 pdynamo (\(FieldInfo name) (I val) -> K ((T.pack name,) <$> dEncode val)) ns xs
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
rangeData (RangeEquals a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeLessThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThan a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeGreaterThanE a) = [(rangeKey, dScalarEncode a)]
rangeData (RangeBetween s e) = [(rangeStart, dScalarEncode s), (rangeEnd, dScalarEncode e)]
rangeData (RangeBeginsWith a) = [(rangeKey, dScalarEncode a)]
