-- Query Inv. Movement DC Summary v11 :  perbaikan pada SO plus Minus dan Non konsinyasi

USE [PB_DC]

DECLARE @date1 date='2025-06-01'    -- isi periode awal penarikan data
DECLARE @date2 date='2025-06-30'    -- isi periode akhir penarikan data
DECLARE @iddc int= 1                -- isi berdasarkan iddc

EXEC sp_SMI_InvMove_DC_summary_view_v11 @date1,@date2,@iddc;

/****************************************************************************/

-- Query Inv. Movement Toko Summary v10 : perbaikan pada SO plus Minus dan Non konsinyasi

USE PB_DC;
DECLARE @date1 date='2025-03-01'    -- isi periode awal penarikan data
DECLARE @date2 date='2025-03-31'    -- isi periode akhir penarikan data
Declare @idcabang int=2                -- isi berdasarkan idcabang
Declare @kodestatustoko varchar='R'    -- isi berdasarkan Kodestatustoko

EXEC sp_SMI_InvMove_Toko_Summary_view_10 @date1,@date2,@kodestatustoko,@idcabang;

/****************************************************************************/

-- Query Inv. Movement Toko Detail v10 : perbaikan pada SO plus Minus dan Non konsinyasi

Use PB_DC;
Declare @date1 date ='2025-03-01'        -- diganti berdasarkan tanggal awal bulan penarikan data
Declare @date2 date ='2025-03-01'        -- diganti berdasarkan tanggal akhir bulan penarikan data
Declare @kodestatustoko varchar='R'        -- diganti berdasarkan kodestatustoko data yang diingikan
Declare @idcabang int=2                    -- diganti berdasarkan idcabang data yang diingikan

EXEC sp_SMI_InvMove_Toko_Detail_view_10 @date1,@date2,@kodestatustoko,@idcabang;

/****************************************************************************/ 