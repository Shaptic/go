-- +migrate Up

-- CAP-40 signed payload strkeys are 114 characters long
ALTER TABLE accounts_signers
  ALTER COLUMN signer TYPE character varying(114);

-- +migrate Down
ALTER TABLE accounts_signers
  ALTER COLUMN signer TYPE character varying(64);
