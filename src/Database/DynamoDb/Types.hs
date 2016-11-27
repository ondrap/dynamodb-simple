{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.DynamoDb.Types where

import           Control.Lens                ((.~), (^.))
import qualified Data.ByteString             as BS
import           Data.Double.Conversion.Text (toShortest)
import           Data.Function               ((&))
import           Data.Monoid                 ((<>))
import           Data.Proxy
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

class DynamoEncodable a where
  dEncode :: a -> AttributeValue
  dDecode :: AttributeValue -> Maybe a

instance DynamoEncodable Int where
  dEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
  dDecode attr = attr ^. D.avN >>= readMaybe . T.unpack
instance DynamoEncodable Double where
  dEncode num = attributeValue & D.avN .~ (Just $ toShortest num)
  dDecode attr = attr ^. D.avN >>= readMaybe . T.unpack
instance DynamoEncodable Bool where
  dEncode b = attributeValue & D.avBOOL .~ Just b
  dDecode attr = attr ^. D.avBOOL
instance DynamoEncodable T.Text where
  dEncode t = attributeValue & D.avS .~ Just t
  dDecode attr = attr ^. D.avS

class IsNotNumber a
instance IsNotNumber T.Text
instance IsNotNumber BS.ByteString

data RangeOper a where
  Equal :: a -> RangeOper a
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
rangeOper (Equal _) n = "#" <> n <> " = " <> rangeKey
rangeOper (LessThan _) n = "#" <> n <> " < " <> rangeKey
rangeOper (LessThanE _) n = "#" <> n <> " <= " <> rangeKey
rangeOper (GreaterThan _) n = "#" <> n <> " > " <> rangeKey
rangeOper (GreaterThanE _) n = "#" <> n <> " >= " <> rangeKey
rangeOper (Between _ _) n = "#" <> n <> " BETWEEN " <> rangeStart <> " AND " <> rangeEnd
rangeOper (BeginsWith _) n = "begins_with(#" <> n <> ", " <> rangeKey <> ")"

rangeData :: DynamoEncodable a => RangeOper a -> [(T.Text, AttributeValue)]
rangeData (Equal a) = [(rangeKey, dEncode a)]
rangeData (LessThan a) = [(rangeKey, dEncode a)]
rangeData (LessThanE a) = [(rangeKey, dEncode a)]
rangeData (GreaterThan a) = [(rangeKey, dEncode a)]
rangeData (GreaterThanE a) = [(rangeKey, dEncode a)]
rangeData (Between s e) = [(rangeStart, dEncode s), (rangeEnd, dEncode e)]
rangeData (BeginsWith a) = [(rangeKey, dEncode a)]
