CREATE TABLE stg_investigations
(
  bus_dt           varchar(20),
  prpc_lob_seq_id  varchar(10),
  inqr_id          varchar(100),
  client_id        varchar(50),
  calc_rslv_dt     varchar(20),
  case_entr_dt     varchar(20),
  frst_rslv_dt     varchar(20),
  last_ropned_dt   varchar(20),
  ropn_cn          varchar(10),
  inqr_amt         varchar(100),
  inqr_amt_ccy     varchar(20),
  case_own_nm      varchar(100)
);


CREATE TABLE dim_clients
(
  client_id           varchar(50) NOT NULL,
  client_name         varchar(100)
);

CREATE TABLE dim_clients_top
(
  client_id           varchar(50) NOT NULL,
  top_client_ind      varchar(1)
);

CREATE TABLE fact_detail
(
  bus_dt           date,
  prpc_lob_seq_id  varchar(10),
  inqr_id          varchar(100),
  client_id        varchar(50),
  calc_rslv_dt     date,
  case_entr_dt     date,
  frst_rslv_dt     date,
  last_ropned_dt   date,
  ropn_cn          smallint,
  inqr_amt         varchar(100),
  inqr_amt_ccy     varchar(20),
  case_own_nm      varchar(100),
  first_tat        smallint,
  last_tat         smallint,
  top_client_ind   varchar(1)
);

CREATE TABLE fact_summ
(
  bus_dt           date,
  client_id        varchar(50),
  total_tat        smallint,
  avg_tat          smallint,
  total_value      bigint,
  rslv_cnt         smallint
);
