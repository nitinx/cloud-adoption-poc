INSERT INTO fact_detail (bus_dt, prpc_lob_seq_id, inqr_id, client_id, calc_rslv_dt, case_entr_dt, frst_rslv_dt, last_ropned_dt, ropn_cn, inqr_amt, inqr_amt_ccy, case_own_nm, first_tat, last_tat, top_client_ind)
SELECT TO_DATE (bus_dt, 'DD/MM/YYYY') AS bus_dt,
       CAST(prpc_lob_seq_id as smallint) AS prpc_lob_seq_id,
       inqr_id AS inqr_id,
       public.stg_investigations.client_id AS client_id,
       TO_DATE (calc_rslv_dt, 'DD/MM/YYYY') AS calc_rslv_dt,
       TO_DATE (case_entr_dt, 'DD/MM/YYYY') AS case_entr_dt,
       TO_DATE (frst_rslv_dt, 'DD/MM/YYYY') AS frst_rslv_dt,
       TO_DATE (last_ropned_dt, 'DD/MM/YYYY') AS last_ropned_dt,
       CAST(ropn_cn as smallint) AS ropn_cn,
       CAST(inqr_amt as bigint) AS inqr_amt,
       inqr_amt_ccy AS inqr_amt_ccy,
       case_own_nm AS case_own_nm,
       TO_DATE (frst_rslv_dt, 'DD/MM/YYYY') - TO_DATE (case_entr_dt, 'DD/MM/YYYY') AS first_tat,
       TO_DATE (calc_rslv_dt, 'DD/MM/YYYY') - TO_DATE (last_ropned_dt, 'DD/MM/YYYY') AS last_tat,
       top_client_ind AS top_client_ind
  FROM public.stg_investigations
  LEFT OUTER JOIN public.dim_clients_top 
ON (public.stg_investigations.client_id = public.dim_clients_top.client_id) limit 10;
   

INSERT INTO fact_summ (bus_dt, client_id, total_tat, avg_tat, total_value, total_value, rslv_cnt)
SELECT bus_dt AS bus_dt,
       client_id AS client_id,
       SUM(first_tat + last_tat) AS total_tat,
       AVG(first_tat + last_tat) AS avg_tat,
       SUM(inqr_amt) AS total_value,
       COUNT(inqr_id) AS rslv_cnt
  FROM public.fact_detail 
  GROUP BY bus_dt, client_id limit 10;
 