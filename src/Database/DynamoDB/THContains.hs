{-# LANGUAGE TemplateHaskell #-}
module Database.DynamoDB.THContains where

import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT, tell)
import           Data.Function                   ((&))
import           Data.List                       (find)
import           Language.Haskell.TH

import           Database.DynamoDB.Class         (ContainsTableKey (..))
import           Database.DynamoDB.THLens        (getFieldNames, whenJust)

-- | Create ContainsTableKey instance
createContainsTableKey :: (String -> String) -> Name -> [String] -> Name -> WriterT [Dec] Q ()
createContainsTableKey translate parent pkeyfields item = do
    tblFieldNames <- getFieldNames item id
    let pfields = pkeyfields & map (\pname -> (find (\(n,_) -> translate n == pname) tblFieldNames))
                             & sequence
    whenJust pfields $ \pkey -> do
        case pkey of
          [(fname, typ)] ->
              lift [d|
                instance ContainsTableKey $(conT item) $(conT parent) $(pure typ) where
                    dTableKey = $(varE (mkName fname))
                |] >>= tell
          [(fname1, typ1), (fname2, typ2)] ->
              lift [d|
                instance ContainsTableKey $(conT item) $(conT parent) ($(pure typ1), $(pure typ2)) where
                    dTableKey a = ($(varE (mkName fname1)) a, $(varE (mkName fname2)) a)
                |] >>= tell
          _ -> fail "Unexpected pkey length, internal error"
