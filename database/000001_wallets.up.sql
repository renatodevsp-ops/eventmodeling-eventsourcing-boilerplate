CREATE TABLE public.wallets (
	id bigserial NOT NULL,
	wallet_id varchar NULL,
	CONSTRAINT wallets_pkey PRIMARY KEY (id),
	CONSTRAINT wallets_wallet_id_key UNIQUE (wallet_id)
);