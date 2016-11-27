{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.DynamoDb.Types (
    DynamoEncodable(..)
  , DynamoScalar(..)
  , RangeOper(..)
  , rangeOper
  , rangeData
) where

import           Control.Lens                ((.~), (^.))
import qualified Data.ByteString             as BS
import           Data.Double.Conversion.Text (toShortest)
import           Data.Function               ((&))
import           Data.HashMap.Strict         (HashMap)
import           Data.Monoid                 ((<>))
import           Data.Proxy
import qualified Data.Set                    as Set
import qualified Data.Text                   as T
import           Network.AWS.DynamoDB.Types  (AttributeValue,
                                              ScalarAttributeType,
                                              attributeValue)
import qualified Network.AWS.DynamoDB.Types  as D
import           Text.Read                   (readMaybe)

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


class IsNotNumber a
instance IsNotNumber T.Text
instance IsNotNumber BS.ByteString

data RangeOper a where
  Equals :: a -> RangeOper a
  LessThan :: a -> RangeOper a
  LessThanE :: a -> RangeOper a
  GreaterThan :: a -> RangeOper a
  GreaterThanE :: a -> RangeOper a
  Between :: a -> a -> RangeOper a
  BeginsWith :: (IsNotNumber a) => a -> RangeOper a

rangeKey :: T.Text
rangeKey = ":rangekey"

rangeStart :: T.Text
rangeStart = ":rangeStart"

rangeEnd :: T.Text
rangeEnd = ":rangeEnd"

rangeOper :: RangeOper a -> T.Text -> T.Text
rangeOper (Equals _) n = "#" <> n <> " = " <> rangeKey
rangeOper (LessThan _) n = "#" <> n <> " < " <> rangeKey
rangeOper (LessThanE _) n = "#" <> n <> " <= " <> rangeKey
rangeOper (GreaterThan _) n = "#" <> n <> " > " <> rangeKey
rangeOper (GreaterThanE _) n = "#" <> n <> " >= " <> rangeKey
rangeOper (Between _ _) n = "#" <> n <> " BETWEEN " <> rangeStart <> " AND " <> rangeEnd
rangeOper (BeginsWith _) n = "begins_with(#" <> n <> ", " <> rangeKey <> ")"

rangeData :: DynamoScalar a => RangeOper a -> [(T.Text, AttributeValue)]
rangeData (Equals a) = [(rangeKey, dEncode a)]
rangeData (LessThan a) = [(rangeKey, dEncode a)]
rangeData (LessThanE a) = [(rangeKey, dEncode a)]
rangeData (GreaterThan a) = [(rangeKey, dEncode a)]
rangeData (GreaterThanE a) = [(rangeKey, dEncode a)]
rangeData (Between s e) = [(rangeStart, dEncode s), (rangeEnd, dEncode e)]
rangeData (BeginsWith a) = [(rangeKey, dEncode a)]
