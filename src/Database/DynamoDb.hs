{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.DynamoDb (
    DynamoException(..)
  , Consistency(..)
  , putItem
  , getItem
) where

import           Control.Lens                 (Iso', iso, (.~), (^.))
import           Control.Monad                (void)
import           Control.Monad.Catch          (throwM)
import           Data.Function                ((&))
import           Data.Monoid                  ((<>))
import           Data.Proxy
import qualified Data.Text                    as T
import           Generics.SOP
import           Network.AWS
import qualified Network.AWS.DynamoDB.GetItem as D

import           Database.DynamoDb.Class
import           Database.DynamoDb.Filter
import           Database.DynamoDb.Types

-- | Parameter for queries involving read consistency settings
data Consistency = Eventually | Strongly
  deriving (Show)

-- | Lens to help set consistency
consistencyL :: Iso' (Maybe Bool) Consistency
consistencyL = iso tocons fromcons
  where
    tocons (Just True) = Strongly
    tocons _ = Eventually
    fromcons Strongly = Just True
    fromcons Eventually = Just False

-- | Save item into the database
putItem :: (MonadAWS m, DynamoTable a r t) => a -> m ()
putItem item = void $ send (dPutItem item)

-- | Read item from the database; primary key is either a hash key or (hash,range) tuple depending on the table
getItem :: forall m a r t key hash rest.
    (MonadAWS m, DynamoTable a r t, RecordOK (Code a) r, Code a ~ '[ key ': hash ': rest], ItemOper a r, All2 DynamoEncodable (Code a))
    => Consistency -> PrimaryKey (Code a) r -> m (Maybe a)
getItem consistency key = do
  let cmd = dGetItem (Proxy :: Proxy a) key & D.giConsistentRead . consistencyL .~ consistency
  rs <- send cmd
  let result = rs ^. D.girsItem
  if | null result -> return Nothing
     | otherwise ->
          case gdDecode result of
              Just res -> return (Just res)
              Nothing -> throwM (DynamoException $ "Cannot decode item: " <> T.pack (show result))

-- | Query item in a database


-- | Query item in a database using range key
