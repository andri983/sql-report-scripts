--USE [PB_DC]
--GO

--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO


--CREATE PROCEDURE [dbo].[GetSMIInventoryMovementCONsignmentDC]

--(
--	@date1 date,   
--    @date2 date,
--	@iddc int
	
--)

--AS
--SET TRANSACTION ISOLATION LEVEL READ unCOMMITTED ; 

--set nocount ON

--BEGIN

DECLARE @date1 DATE = '2025-09-01';
DECLARE @date2 DATE = '2025-09-30';
DECLARE @iddc INT = 1;
	SELECT
	DATEPART(Year,@date1) AS Tahun, 
	DATEPART(mONth,@date1) AS Bulan,	
	t.iddc,
	t.kodeproduk,
	t.namaPanjang AS DescriptiON,
	t.Category,
	t.COA,
	SUM(t.ttl_qty_SaldoAwal) AS ttl_qty_SaldoAwal,
	SUM(t.ttl_Value_SaldoAwal) AS ttl_Value_SaldoAwal,
	SUM(t.qty_LPB_Supp) AS qty_LPB_Supp,
	SUM(t.Value_LPB_Supp) AS Value_LPB_Supp,

	SUM(t.Qty_Retur_Reg) AS Qty_Retur_Reg,
	SUM(t.Value_Retur_Reg) AS Value_Retur_Reg,

	SUM(t.Qty_Retur_FRC) AS Qty_Retur_FRC,
	SUM(t.Value_Retur_FRC) AS Value_Retur_FRC,
	SUM(t.Qty_Retur_SLS_Order) AS Qty_Retur_SLS_Order,
	SUM(t.value_Retur_SLS_Order) AS value_Retur_SLS_Order,		

	SUM(t.Qty_DC_IN) AS Qty_DC_IN, 
	SUM(t.Value_DC_IN) AS Value_DC_IN,
	SUM(t.Qty_SO_Plus) AS Qty_SO_Plus,
	SUM(t.Value_SO_plus) AS Value_SO_plus,
	SUM(t.Qty_ReturSupp) AS Qty_ReturSupp,
	SUM(t.Value_ReturSupp) AS Value_ReturSupp,

	SUM(t.Qty_TO_REG) AS Qty_TO_REG,
	SUM(t.Value_TO_REG) AS Value_TO_REG,
	SUM(t.Qty_SLS_Order) AS Qty_SLS_Order,
	SUM(t.value_SLS_Order) AS value_SLS_Order,
	SUM(t.Qty_DO_FRC) AS Qty_DO_FRC,
	SUM(t.Value_DO_FRC) AS Value_DO_FRC,		
	SUM(t.Qty_DC_Out) AS Qty_DC_Out,
	SUM(t.Value_DC_Out) AS Value_DC_Out,	
	
	SUM(t.Qty_SO_Minus) AS Qty_SO_Minus,
	SUM(t.Value_SO_Minus) AS Value_SO_Minus,

	SUM(t.Qty_MGStoBS_minus) AS Qty_MGStoBS_minus,
	SUM(t.Value_MGStoBS_minus) AS Value_MGStoBS_minus,
	SUM(t.Qty_MBStoGS_Plus) AS Qty_MBStoGS_Plus,
	SUM(t.Value_MBStoGS_Plus) AS Value_MBStoGS_Plus,		

	SUM(t.Qty_MGStoBS_plus) AS Qty_MGStoBS_plus,
	SUM(t.Value_MGStoBS_plus) AS Value_MGStoBS_plus,
	SUM(t.Qty_MBStoGS_minus) AS Qty_MBStoGS_minus,
	SUM(t.Value_MBStoGS_minus) AS Value_MBStoGS_minus,

	SUM(t.Qty_MGStoGB_minus) AS Qty_MGStoGB_minus,
	SUM(t.Value_MGStoGB_minus) AS Value_MGStoGB_minus, 
	SUM(t.Qty_MGBtoGS_Plus) AS Qty_MGBtoGS_Plus, 
	SUM(t.Value_MGBtoGS_Plus) AS Value_MGBtoGS_Plus, 

	SUM(t.Qty_MGBtoGS_minus) AS Qty_MGBtoGS_minus, 
	SUM(t.Value_MGBtoGS_minus) AS Value_MGBtoGS_minus, 
	SUM(t.Qty_MGStoGB_Plus) AS Qty_MGStoGB_Plus,
	SUM(t.Value_MGStoGB_Plus) AS Value_MGStoGB_Plus, 

	SUM(t.qty_saldo_hit) AS qty_saldo_hit,
	SUM(t.Value_saldo_hit) AS Value_saldo_hit

	FROM (		
		SELECT 
		m.iddc,m.idproduk,m.kodeproduk,m.Barcode3,m.namaPanjang,m.Category,m.COA,m.idjenisstok,
		SUM(m.ttl_qty_SaldoAwal) AS ttl_qty_SaldoAwal,
		SUM(m.ttl_Value_SaldoAwal) AS ttl_Value_SaldoAwal,
		SUM(m.qty_LPB_Supp) AS qty_LPB_Supp,
		SUM(m.Value_LPB_Supp) AS Value_LPB_Supp,

		SUM(m.Qty_Retur_Reg) AS Qty_Retur_Reg,
		SUM(m.Value_Retur_Reg) AS Value_Retur_Reg,		

		SUM(m.Qty_Retur_FRC) AS Qty_Retur_FRC,
		SUM(m.Value_Retur_FRC) AS Value_Retur_FRC,

		SUM(m.Qty_Retur_SLS_Order) AS Qty_Retur_SLS_Order,
		SUM(m.value_Retur_SLS_Order) AS value_Retur_SLS_Order,		

		SUM(m.Qty_DC_IN) AS Qty_DC_IN, 
		SUM(m.Value_DC_IN) AS Value_DC_IN,
		SUM(m.Qty_SO_Plus) AS Qty_SO_Plus,
		SUM(m.Value_SO_plus) AS Value_SO_plus,

		SUM(m.Qty_ReturSupp) AS Qty_ReturSupp,
		SUM(m.Value_ReturSupp) AS Value_ReturSupp,

		SUM(m.Qty_TO_REG) AS Qty_TO_REG,
		SUM(m.Value_TO_REG) AS Value_TO_REG,
		SUM(m.Qty_SLS_Order) AS Qty_SLS_Order,
		SUM(m.value_SLS_Order) AS value_SLS_Order,
		SUM(m.Qty_DO_FRC) AS Qty_DO_FRC,
		SUM(m.Value_DO_FRC) AS Value_DO_FRC,		
		SUM(m.Qty_DC_Out) AS Qty_DC_Out,
		SUM(m.Value_DC_Out) AS Value_DC_Out,	
	
		SUM(m.Qty_SO_Minus) AS Qty_SO_Minus,
		SUM(m.Value_SO_Minus) AS Value_SO_Minus,

		SUM(m.Qty_MGStoBS_minus) AS Qty_MGStoBS_minus,
		SUM(m.Value_MGStoBS_minus) AS Value_MGStoBS_minus,
		SUM(m.Qty_MBStoGS_Plus) AS Qty_MBStoGS_Plus,
		SUM(m.Value_MBStoGS_Plus) AS Value_MBStoGS_Plus,		

		SUM(m.Qty_MGStoBS_plus) AS Qty_MGStoBS_plus,
		SUM(m.Value_MGStoBS_plus) AS Value_MGStoBS_plus,
		SUM(m.Qty_MBStoGS_minus) AS Qty_MBStoGS_minus,
		SUM(m.Value_MBStoGS_minus) AS Value_MBStoGS_minus,

		SUM(m.Qty_MGStoGB_minus) AS Qty_MGStoGB_minus,
		SUM(m.Value_MGStoGB_minus) AS Value_MGStoGB_minus, 
		SUM(m.Qty_MGBtoGS_Plus) AS Qty_MGBtoGS_Plus, 
		SUM(m.Value_MGBtoGS_Plus) AS Value_MGBtoGS_Plus, 

		SUM(m.Qty_MGBtoGS_minus) AS Qty_MGBtoGS_minus, 
		SUM(m.Value_MGBtoGS_minus) AS Value_MGBtoGS_minus, 
		SUM(m.Qty_MGStoGB_Plus) AS Qty_MGStoGB_Plus,
		SUM(m.Value_MGStoGB_Plus) AS Value_MGStoGB_Plus, 

		CASE 
		WHEN m.idjenisstok=1 
			then (SUM(m.ttl_qty_SaldoAwal)+SUM(m.qty_LPB_Supp)+SUM(m.Qty_Retur_Reg)+SUM(m.Qty_Retur_FRC)+SUM(m.Qty_Retur_SLS_Order)+SUM(m.Qty_DC_IN)+SUM(m.Qty_SO_Plus)
			-SUM(m.Qty_ReturSupp)-SUM(m.Qty_TO_REG)-SUM(m.Qty_SLS_Order)-SUM(m.Qty_DO_FRC)-SUM(m.Qty_DC_Out)-SUM(m.Qty_SO_Minus)+SUM(m.Qty_MBStoGS_Plus)-SUM(m.Qty_MGStoBS_minus)
			-SUM(m.Qty_MGStoGB_minus)+SUM(m.Qty_MGBtoGS_Plus))

		WHEN m.idjenisstok=2 then (SUM(m.ttl_qty_SaldoAwal)+SUM(m.qty_LPB_Supp)+SUM(m.Qty_Retur_Reg)+SUM(m.Qty_Retur_FRC)+SUM(m.Qty_Retur_SLS_Order)+SUM(m.Qty_DC_IN)+SUM(m.Qty_SO_Plus)
			-SUM(m.Qty_ReturSupp)-SUM(m.Qty_TO_REG)-SUM(m.Qty_SLS_Order)-SUM(m.Qty_DO_FRC)-SUM(m.Qty_DC_Out)-SUM(m.Qty_SO_Minus)+SUM(m.Qty_MGStoBS_plus)-SUM(m.Qty_MBStoGS_minus))
	
		WHEN m.idjenisstok=3 
			then (SUM(m.ttl_qty_SaldoAwal)+SUM(m.qty_LPB_Supp)+SUM(m.Qty_Retur_Reg)+SUM(m.Qty_Retur_FRC)+SUM(m.Qty_Retur_SLS_Order)+SUM(m.Qty_DC_IN)+SUM(m.Qty_SO_Plus)
			-SUM(m.Qty_ReturSupp)-SUM(m.Qty_TO_REG)-SUM(m.Qty_SLS_Order)-SUM(m.Qty_DO_FRC)-SUM(m.Qty_DC_Out)-SUM(m.Qty_SO_Minus)+SUM(m.Qty_MBStoGS_Plus)-SUM(m.Qty_MGStoBS_minus)
			-SUM(m.Qty_MGBtoGS_minus)+SUM(m.Qty_MGStoGB_Plus))
		ELSE 0 END AS qty_saldo_hit,

	CASE 
		WHEN m.idjenisstok=1 
			then (SUM(m.ttl_Value_SaldoAwal)+SUM(m.Value_LPB_Supp)+SUM(m.Value_Retur_Reg)+SUM(m.Value_Retur_FRC)+SUM(m.value_Retur_SLS_Order)+SUM(m.Value_DC_IN)+SUM(m.Value_SO_Plus)
			-SUM(m.Value_ReturSupp)-SUM(m.Value_TO_REG)-SUM(m.value_SLS_Order)-SUM(m.Value_DO_FRC)-SUM(m.Value_DC_Out)-SUM(m.Value_SO_Minus)+SUM(m.Value_MBStoGS_Plus)-SUM(m.Value_MGStoBS_minus)
			-SUM(m.Value_MGStoGB_minus)+SUM(m.Value_MGBtoGS_Plus))
	
		WHEN m.idjenisstok=2 then (SUM(m.ttl_Value_SaldoAwal)+SUM(m.Value_LPB_Supp)+SUM(m.Value_Retur_Reg)+SUM(m.Value_Retur_FRC)+SUM(m.value_Retur_SLS_Order)+SUM(m.Value_DC_IN)+SUM(m.Value_SO_Plus)
			-SUM(m.Value_ReturSupp)-SUM(m.Value_TO_REG)-SUM(m.value_SLS_Order)-SUM(m.Value_DO_FRC)-SUM(m.Value_DC_Out)-SUM(m.Value_SO_Minus)+SUM(m.Value_MGStoBS_plus)-SUM(m.Value_MBStoGS_minus))
	
		WHEN m.idjenisstok=3 
			then (SUM(m.ttl_Value_SaldoAwal)+SUM(m.Value_LPB_Supp)+SUM(m.Value_Retur_Reg)+SUM(m.Value_Retur_FRC)+SUM(m.value_Retur_SLS_Order)+SUM(m.Value_DC_IN)+SUM(m.Value_SO_Plus)
			-SUM(m.Value_ReturSupp)-SUM(m.Value_TO_REG)-SUM(m.value_SLS_Order)-SUM(m.Value_DO_FRC)-SUM(m.Value_DC_Out)-SUM(m.Value_SO_Minus)+SUM(m.Value_MBStoGS_Plus)-SUM(m.Value_MGStoBS_minus)
			-SUM(m.Value_MGBtoGS_minus)+SUM(m.Value_MGStoGB_Plus))
		 ELSE 0 END AS Value_saldo_hit
 
		FROM (	
			SELECT -- Saldo Awal--
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,D.idjenisstok,
			SUM(ISNULL(Qty,0)) AS ttl_qty_SaldoAwal,
			(SUM(ISNULL(Qty,0)*ISNULL(hpp,0))) AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order,
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM pb_dc.dbo.saldostokprodukdc AS D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
			CONVERT(date,D.tglsaldo)= CONVERT(date,(dateadd (day,-1, @date1))) 
			and idDc =@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,Y.name,mp.namaPanjang,z.namadepartement,D.idjenisstok
		UNION ALL
			SELECT --- LPB_DC Dari Supplier -------------
			B.iddc,A.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			SUM(ISNULL(A.qty,0)) AS qty_LPB_Supp,
			SUM(ISNULL(A.rpLPB,0)) AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order,
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM pb_dc.dbo.V_SMI_LpbDCDariSupplier AS A WITH (NOLOCK)
			INNER JOIN pb_dc.dbo.mstdc AS B WITH (NOLOCK) ON A.kodedc=B.kodedc
			INNER JOIN MstProduk mp ON mp.idProduk = A.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			A.statusdata in (1,2)
			and CONVERT(date,A.tglApprove) between @date1 and @date2
			And B.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY B.iddc,mp.Barcode3,mp.kodeproduk,A.idproduk,Y.name,mp.namaPanjang,z.namadepartement
		UNION ALL
			SELECT --- Retur dr Toko Reguler ---
			A.iddc,A.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,A.idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			SUM(ISNULL(A.qty_retur,0)) AS Qty_Retur_Reg,
			SUM(ISNULL(A.Netto,0)) AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM Pb_DC.dbo.V_SMI_ReturdanLPBTokoKeDc AS A WITH (NOLOCK)
			INNER JOIN PB_DC.dbo.msttoko AS B WITH (NOLOCK) ON A.kodetoko=B.kodetoko
			INNER JOIN MstProduk mp ON mp.idProduk = A.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
			B.kodestatustoko ='R' -- update 12 Nov. 2021
			and A.statusdata=2
			and CONVERT(date,tglLPB) between @date1 and @date2
			and A.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY A.iddc,mp.Barcode3,mp.kodeproduk,A.idproduk,Y.name,mp.namaPanjang,z.namadepartement,A.idjenisstok	
		UNION ALL
			SELECT --- Retur dr Frainchise ---
			A.iddc,A.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,A.idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			SUM(ISNULL(A.qty_retur,0)) AS Qty_Retur_FRC, 
			(SUM(ISNULL(A.qty_retur,0)*ISNULL(Hppdc,0))) AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM pb_dc.dbo.V_SMI_ReturdanLPBTokoKeDc AS A WITH (NOLOCK)
			INNER JOIN PB_DC.dbo.msttoko AS B WITH (NOLOCK) ON A.kodetoko=B.kodetoko
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = A.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
			A.statusdata=2 
			and B.kodestatustoko <>'R'
			and CONVERT(date,tglLPB) between @date1 and @date2
			and A.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY A.iddc,mp.Barcode3,mp.kodeproduk,A.idproduk,mp.namaPanjang,z.namadepartement,Y.name,A.idjenisstok	
		UNION ALL
			SELECT --- Retur Sales Order ---
				A.iddc,A.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,A.idjenisstok,
				0 AS ttl_qty_SaldoAwal,
				0 AS ttl_Value_SaldoAwal,
				0 AS qty_LPB_Supp,
				0 AS Value_LPB_Supp,
				0 AS Qty_Retur_Reg,
				0 AS Value_Retur_Reg,
				0 AS Qty_Retur_FRC, 
				0 AS Value_Retur_FRC,
				SUM(ISNULL(A.qty,0)) AS Qty_Retur_SLS_Order, 
				(SUM(ISNULL(A.qty,0)*ISNULL(Hppshipment,0))) AS Value_Retur_SLS_Order,
				0 AS Qty_DC_IN, 
				0 AS Value_DC_IN,
				0 AS Qty_SO_Plus,
				0 AS Value_SO_plus,
				0 AS Qty_ReturSupp,
				0 AS Value_ReturSupp,
				0 AS Qty_TO_REG,
				0 AS Value_TO_REG,
				0 AS Qty_SLS_Order,
				0 AS Value_SLS_Order,
				0 AS Qty_DO_FRC,
				0 AS Value_DO_FRC,		
				0 AS Qty_DC_Out,
				0 AS Value_DC_Out,		
				0 AS Qty_SO_Minus,
				0 AS Value_SO_Minus,

				0 AS Qty_MGStoBS_minus,
				0 AS Value_MGStoBS_minus,
				0 AS Qty_MBStoGS_Plus,
				0 AS Value_MBStoGS_Plus,		

				0 AS Qty_MGStoBS_plus,
				0 AS Value_MGStoBS_plus,
				0 AS Qty_MBStoGS_minus,
				0 AS Value_MBStoGS_minus,

				0 AS Qty_MGStoGB_minus,
				0 AS Value_MGStoGB_minus, 
				0 AS Qty_MGBtoGS_Plus, 
				0 AS Value_MGBtoGS_Plus, 

				0 AS Qty_MGBtoGS_minus, 
				0 AS Value_MGBtoGS_minus, 
				0 AS Qty_MGStoGB_Plus,
				0 AS Value_MGStoGB_Plus 

				FROM pb_dc.dbo.V_SMI_ReturSalesOrder AS A WITH (NOLOCK)
				INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = A.idProduk
				LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
				INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
				INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
				INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
				WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
				A.statusreceipt=1
				and CONVERT(date,A.TglReceipt) between @date1 and @date2
				and A.iddc=@iddc
				and ISNULL(x1.NONStock,0)=0
				and mp.flagkONsinyasi=1
				GROUP BY A.iddc,mp.Barcode3,mp.kodeproduk,A.idproduk,mp.namaPanjang,z.namadepartement,Y.name,A.idjenisstok
		UNION ALL
			SELECT --- Mutasi DC IN ---
			D.idDcPenerima AS iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC, 
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			SUM(ISNULL(qty,0)) AS Qty_DC_IN,
			SUM(ISNULL(subTotal,0)) AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_Mutasi_DC_IN D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
			D.Statusdata=1
			and CONVERT(date,TglApprove) between @date1 and @date2
			and idDcPenerima=@iddc
			and idDcPengirim<>@idDc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.idDcPenerima,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name			
		UNION ALL
			SELECT --- Retur DC ke Supplier 
			B.iddc,A.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,A.idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC, 
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN,
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			SUM(ISNULL(A.qty,0)) AS Qty_ReturSupp,
			(SUM(ISNULL(A.qty,0)*ISNULL(A.harga,0))) AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM pb_dc.dbo.V_SMI_ReturDcKeSupplier AS A WITH (NOLOCK)
			INNER JOIN pb_dc.dbo.mstdc AS B WITH (NOLOCK) ON A.kodedc=B.kodedc
			INNER JOIN MstProduk mp ON mp.idProduk = A.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and
			A.statusdata=1
			and CONVERT(date,A.tglApprove) between @date1 and @date2
			and B.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY B.iddc,mp.Barcode3,mp.kodeproduk,A.idproduk,Y.name,mp.namaPanjang,z.namadepartement,A.idjenisstok	
		UNION ALL
			SELECT  --- DO (TO/SJ) ke Toko Reguler ---
			A.iddc,A.idproduk,C.kodeproduk,C.Barcode3,C.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC, 
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN,
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			(SUM(ISNULL(A.qtyTO,0))) AS Qty_TO_REG,
			(SUM(ISNULL(A.Subtotal,0))) AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,
			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_TO_ke_Toko_ManualdanOtomatis_Cab AS A WITH (NOLOCK)
			INNER JOIN PB_DC.dbo.msttoko AS B WITH (NOLOCK) ON A.kodetoko=B.kodetoko
			INNER JOIN PB_DC.dbo.mstproduk AS C WITH (NOLOCK) ON A.idproduk=C.idproduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = C.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and C.statusdata<>9 and C.idjenisproduk<>4 and
			B.kodestatustoko='R' -- update 12 Nov. 2021
			and A.statusdata <>0	
			and CONVERT(date,A.tglApprove) between @date1 and @date2
			and CONVERT(date,A.tglApprove) is not null --update tgl 21 Mei 2022 by Toha
			and A.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and c.flagkONsinyasi=1
			GROUP BY A.iddc,C.Barcode3,C.kodeproduk,A.idproduk,Y.name,C.namaPanjang,z.namadepartement
		UNION ALL
			SELECT -- Sales Order (modul baru di DC) penjualan lansung dr DC -- Update 04 Agt 2023 by Toha --
				--a.tglshipment,
				a.iddc,
				b.idproduk,
				C.kodeproduk,
				C.Barcode3,
				C.namaPanjang,
				z.namadepartement AS Category,
				Y.name AS COA,
				a."IdJenisStock",
				0 AS ttl_qty_SaldoAwal,
				0 AS ttl_Value_SaldoAwal,
				0 AS qty_LPB_Supp,
				0 AS Value_LPB_Supp,
				0 AS Qty_Retur_Reg,
				0 AS Value_Retur_Reg,
				0 AS Qty_Retur_FRC, 
				0 AS Value_Retur_FRC,
				0 AS Qty_Retur_SLS_Order, 
				0 AS Value_Retur_SLS_Order,
				0 AS Qty_DC_IN,
				0 AS Value_DC_IN,
				0 AS Qty_SO_Plus,
				0 AS Value_SO_plus,
				0 AS Qty_ReturSupp,
				0 AS Value_ReturSupp,
				0 AS Qty_TO_REG,
				0 AS Value_TO_REG,
				(SUM(ISNULL(b.qty,0))) AS Qty_SLS_Order,
				(SUM(ISNULL((b.qty*b.hpp),0))) AS Value_SLS_Order,
				0 AS Qty_DO_FRC,
				0 AS Value_DO_FRC,
				0 AS Qty_DC_Out,
				0 AS Value_DC_Out,		
				0 AS Qty_SO_Minus,
				0 AS Value_SO_Minus,
				0 AS Qty_MGStoBS_minus,
				0 AS Value_MGStoBS_minus,
				0 AS Qty_MBStoGS_Plus,
				0 AS Value_MBStoGS_Plus,
				0 AS Qty_MGStoBS_plus,
				0 AS Value_MGStoBS_plus,
				0 AS Qty_MBStoGS_minus,
				0 AS Value_MBStoGS_minus,
				0 AS Qty_MGStoGB_minus,
				0 AS Value_MGStoGB_minus, 
				0 AS Qty_MGBtoGS_Plus, 
				0 AS Value_MGBtoGS_Plus, 
				0 AS Qty_MGBtoGS_minus, 
				0 AS Value_MGBtoGS_minus, 
				0 AS Qty_MGStoGB_Plus,
				0 AS Value_MGStoGB_Plus 			
			FROM PB_DC.dbo.shipmentHeader AS a WITH (NOLOCK)
				join PB_DC.dbo.shipmentDetail AS b WITH (NOLOCK) ON b.iddc=a.iddc and b.nomoruso=a.nomoruso 
						and b.nomorso=a.nomorso and b.nomorshipment=a.nomorshipment
				join PB_DC.dbo.mstproduk AS c WITH (NOLOCK) ON  c.idproduk=b.idProduk
				LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=B.idproduk
				join PB_DC.dbo.MstDepartement z ON z.idDepartement = C.idDepartement 
				Join PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
				Join PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE 
				x.Keterangan='Persediaan' and C.statusdata<>9 and C.idjenisproduk<>4 
				and A.statusdata=1
				and CONVERT(date,a.tglshipment) between @date1 and @date2
				and a.iddc=@iddc
				and ISNULL(x1.NONStock,0)=0
				and c.flagkONsinyasi=1
			GROUP BY
				a.iddc,
				b.idproduk,
				C.kodeproduk,
				C.Barcode3,
				C.namaPanjang,
				z.namadepartement,
				Y.name,
				a."IdJenisStock",
				b.hpp
		UNION ALL
			SELECT  --- DO (TO) ke Frainchise ---
			A.iddc,A.idproduk,C.kodeproduk,C.Barcode3,C.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC, 
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN,
			0 AS Value_DC_IN,
			0 AS Qty_SO_Plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			(SUM(ISNULL(A.qtyTO,0))) AS Qty_DO_FRC,
			(SUM(ISNULL(A.Subtotal,0))) AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_Minus,
			0 AS Value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM pb_dc.dbo.V_SMI_TO_ke_Toko_ManualdanOtomatis_Cab AS A WITH (NOLOCK)
			INNER JOIN PB_DC.dbo.msttoko AS B WITH (NOLOCK) ON A.kodetoko=B.kodetoko
			INNER JOIN PB_DC.dbo.mstproduk AS C WITH (NOLOCK) ON A.idproduk=C.idproduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = C.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and C.statusdata<>9 and C.idjenisproduk<>4 and
			B.kodestatustoko <>'R' -- update 12 Nov. 2021
			and A.statusdata<>0
			and CONVERT(date,A.tglApprove) between @date1 and @date2
			and CONVERT(date,A.tglApprove) is not null --update tgl 21 Mei 2022 by Toha
			and A.iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and c.flagkONsinyasi=1
			GROUP BY A.iddc,C.Barcode3,C.kodeproduk,A.idproduk,Y.name,C.namaPanjang,z.namadepartement
		UNION ALL		
			SELECT --Mutasi DC Out --
			t.iddc,
			t.idproduk,t.kodeproduk,t.Barcode3,t.namapanjang,t.Category,t.COA,t.idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS qty_SO_Plus,
			0 AS value_SO_Plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,
		
			SUM(t.qty) AS Qty_DC_Out,
			SUM(t.subtotal) AS Value_DC_Out,

			0 AS qty_SO_Minus,
			0 AS value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM (
				SELECT 
				@iddc AS iddc,	
				a.idproduk,c.kodeProduk,c.barcode3,c.namaPanjang,z.namaDepartement AS Category,
				y.name AS COA,1 AS idJenisStok,	
				SUM(a.qty) AS Qty,
				SUM(a.subtotal) AS subtotal
				FROM MutasiDCDetailOut a 
				join MutasiDCHeaderOut b ON b.idDcPengirim=a.idDcPengirim and b.idDcPenerima=a.idDcPenerima and b.NoMutasi=a.NoMutasi
				join mstproduk AS c ON a.idproduk=c.idproduk
				LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
				join pb_dc.dbo.MstDepartement z ON z.idDepartement = c.idDepartement 
				Join PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement and x.idDepartement=c.idDepartement --and x.COACode=F.COACode
				Join PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code 
				WHERE 
				x.Keterangan='Persediaan' and c.idjenisproduk<>4 and 
				CONVERT(date,b.TglApprove) between @date1 and @date2
				and b.idDcPengirim=@iddc
				and b.idDcPenerima<>@idDc
				and ISNULL(x1.NONStock,0)=0
				and c.flagkONsinyasi=1
				GROUP BY 
				a.idproduk,c.kodeProduk,c.barcode3,c.namaPanjang,z.namaDepartement,y.name
			) AS t
			GROUP BY 
			t.idproduk,t.kodeproduk,t.Barcode3,t.namapanjang,t.Category,t.COA,t.idjenisstok,t.iddc
		UNION ALL 	
			SELECT --SO Adjustment
			t.iddc,t.idproduk,t.kodeproduk,t.Barcode3,t.namapanjang,t.Category,t.COA,t.idjenisstok,
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			SUM(t.qty_SO_Plus) AS qty_SO_Plus,
			SUM(t.value_SO_Plus) AS value_SO_Plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,
			(-1*SUM(t.qty_SO_Minus)) AS qty_SO_Minus,
			(-1*SUM(t.value_SO_Minus)) AS value_SO_Minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 
			FROM (
				SELECT 
				m.iddc,m.idproduk,m.kodeProduk,m.barcode3,m.namaPanjang,m.Category,m.COA,m.idJenisStok,
				CASE WHEN m.TtlQty>=0 then SUM(m.TtlQty) ELSE 0 END AS qty_SO_Plus,
				CASE
					WHEN m.TtlQty>0 then SUM(m.TtlValue) 
					WHEN m.TtlQty=0 then 0
					ELSE 0 
				END AS value_SO_Plus,
				CASE WHEN m.TtlQty<0 then SUM(m.TtlQty) ELSE 0 END AS qty_SO_Minus,
				CASE WHEN m.TtlQty<0 then SUM(m.TtlValue) ELSE 0 END AS value_SO_Minus
				FROM (
					SELECT		
					a.iddc,	a.idproduk,c.kodeProduk,c.barcode3,c.namaPanjang,z.namaDepartement AS Category,
					y.name AS COA,b.idJenisStok,		
					SUM(a.qtySelisih) AS TtlQty,
					SUM(a.subtotalselisih) AS TtlValue
					FROM SoAdjusmentDetail a 
					join SoAdjusmentHeader b ON b.idDc=a.idDc and b.nomorSoAdjusment=a.nomorSoAdjusment
					join mstproduk AS c ON a.idproduk=c.idproduk
					LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=A.idproduk
					join pb_dc.dbo.MstDepartement z ON z.idDepartement = c.idDepartement 
					Join PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement and x.idDepartement=c.idDepartement --and x.COACode=F.COACode
					Join PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code 
					WHERE 
					x.Keterangan='Persediaan' and c.idjenisproduk<>4 and 
					CONVERT(date,b.tglsoadjusment) between @date1 and @date2
					and b.statusData=2 
					and a.iddc=@iddc
					and ISNULL(x1.NONStock,0)=0
					and c.flagkONsinyasi=1
					GROUP BY a.iddc,a.idproduk,c.kodeProduk,c.barcode3,c.namaPanjang,z.namaDepartement,y.name,b.idJenisStok
				
				) AS m
				GROUP BY m.iddc,m.idproduk,m.kodeProduk,m.barcode3,m.namaPanjang,m.Category,m.COA,m.idJenisStok,m.TtlQty
			) AS t
			GROUP BY 
			t.iddc,t.idproduk,t.kodeproduk,t.Barcode3,t.namapanjang,t.Category,t.COA,t.idjenisstok
		
		UNION ALL
			SELECT--- Mutasi BS to GS -- Gudang 1 MBG Plus
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(MONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok, --- utk Gdg 1 (GS) 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			SUM(ISNULL(qty,0)) AS Qty_MBStoGS_Plus,
			SUM(ISNULL(subTotal,0)) AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=15 ---MBG 
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name			
		UNION ALL	
			SELECT--- Mutasi GS to BS -- Gudang 1 MGB minus
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(MONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,1 AS idjenisstok, --- utk Gdg 1 (GS) --- 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			SUM(ISNULL(qty,0)) AS Qty_MGStoBS_minus,
			SUM(ISNULL(subTotal,0)) AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc  D  
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=16 ---MGB (idMovment=15-->MBG)
			and statusProses=1
			and idlokasiMovment=2 -- DC
			and CONVERT(date,tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name				
		UNION ALL
			SELECT--- Mutasi GS to BS (BS jd (-) --Gudang 2 Qty_MGStoBS_plus
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(MONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,2 AS idjenisstok, --- utk Gdg 2 (BS) --- 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			SUM(ISNULL(qty,0)) AS Qty_MGStoBS_plus,
			SUM(ISNULL(subTotal,0)) AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=16 ---MGB 
			and statusProses=1
			and idlokasiMovment=2 -- DC
			and CONVERT(date,tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name

		UNION ALL
			SELECT--- Mutasi BS to GS (BS jd (-)) --Gdg 2
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(MONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,2 AS idjenisstok, --- utk Gdg 2 (BS)
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			SUM(ISNULL(qty,0)) AS Qty_MBStoGS_minus,
			SUM(ISNULL(subTotal,0)) AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=15 ---MBG 
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name
		UNION ALL
			SELECT--- Mutasi Booking IN-- di Gdg GS minus
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(mONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,
			1 AS idjenisstok, --- utk Gdg 1 (GS) 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			SUM(ISNULL(qty,0)) AS Qty_MGStoGB_minus,
			SUM(ISNULL(subTotal,0)) AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=22 ---BOOKING IN 
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,D.tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name
		UNION ALL
			SELECT--- Mutasi Booking OUT-- di Gdg GS Plus
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(mONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,
			1 AS idjenisstok, --- utk Gdg 1 (GS) 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			SUM(ISNULL(qty,0)) AS Qty_MGBtoGS_Plus, 
			SUM(ISNULL(subTotal,0)) AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 
				
			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=23 ---BOOKING OUT
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name
	UNION ALL
			SELECT--- --- Mutasi Booking IN-- di GB (GS to GB (+))
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(mONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,
			3 AS idjenisstok, --- utk Gdg 3 (Booking Stock) 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			0 AS Qty_MGBtoGS_minus, 
			0 AS Value_MGBtoGS_minus, 
			SUM(ISNULL(qty,0)) AS Qty_MGStoGB_Plus,
			SUM(ISNULL(subTotal,0)) AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=22 ---BOOKING IN 
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,D.tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and ISNULL(x1.NONStock,0)=0
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name
		UNION ALL
			SELECT--- --- Mutasi Booking OUT-- di GB (GB to GS (-)) 
			--datepart(year,D.tglMutasi) AS tahun,
			--datepart(mONth,D.tglMutasi) AS Bulan,
			D.iddc,D.idproduk,mp.kodeproduk,mp.Barcode3,mp.namaPanjang,z.namadepartement AS Category,Y.name AS COA,3 AS idjenisstok, --- utk Gdg 3 (Booking Stock) 
			0 AS ttl_qty_SaldoAwal,
			0 AS ttl_Value_SaldoAwal,
			0 AS qty_LPB_Supp,
			0 AS Value_LPB_Supp,
			0 AS Qty_Retur_Reg,
			0 AS Value_Retur_Reg,
			0 AS Qty_Retur_FRC,
			0 AS Value_Retur_FRC,
			0 AS Qty_Retur_SLS_Order, 
			0 AS Value_Retur_SLS_Order,
			0 AS Qty_DC_IN, 
			0 AS Value_DC_IN,
			0 AS Qty_SO_plus,
			0 AS Value_SO_plus,
			0 AS Qty_ReturSupp,
			0 AS Value_ReturSupp,
			0 AS Qty_TO_REG,
			0 AS Value_TO_REG,
			0 AS Qty_SLS_Order,
			0 AS Value_SLS_Order,
			0 AS Qty_DO_FRC,
			0 AS Value_DO_FRC,		
			0 AS Qty_DC_Out,
			0 AS Value_DC_Out,		
			0 AS Qty_SO_minus,
			0 AS Value_SO_minus,

			0 AS Qty_MGStoBS_minus,
			0 AS Value_MGStoBS_minus,
			0 AS Qty_MBStoGS_Plus,
			0 AS Value_MBStoGS_Plus,		

			0 AS Qty_MGStoBS_plus,
			0 AS Value_MGStoBS_plus,
			0 AS Qty_MBStoGS_minus,
			0 AS Value_MBStoGS_minus,

			0 AS Qty_MGStoGB_minus,
			0 AS Value_MGStoGB_minus, 
			0 AS Qty_MGBtoGS_Plus, 
			0 AS Value_MGBtoGS_Plus, 

			SUM(ISNULL(qty,0)) AS Qty_MGBtoGS_minus, 
			SUM(ISNULL(subTotal,0)) AS Value_MGBtoGS_minus, 
			0 AS Qty_MGStoGB_Plus,
			0 AS Value_MGStoGB_Plus 

			FROM PB_DC.dbo.V_SMI_MutasiSaldoDc D
			INNER JOIN pb_dc.dbo.MstProduk mp ON mp.idProduk = D.idProduk 
			LEFT JOIN dbo.tblProduknONstock AS x1 WITH (NOLOCK) ON x1.idproduk=D.idproduk
			INNER JOIN pb_dc.dbo.MstDepartement z ON z.idDepartement = mp.idDepartement 
			INNER JOIN PB_DC.dbo.TblDeptCOA AS x WITH (NOLOCK) ON x.idDepartement=z.idDepartement
			INNER JOIN PB_DC.dbo.MstCoa AS y WITH (NOLOCK) ON x.COACode =y.code
			WHERE x.Keterangan='Persediaan' and mp.idjenisproduk<>4 and 
			idMovment=23
			and statusProses=1
			and idlokasiMovment=2
			and CONVERT(date,D.tglMutasi) between @date1 and @date2
			and iddc=@iddc
			and mp.flagkONsinyasi=1
			GROUP BY D.iddc,mp.Barcode3,mp.kodeproduk,D.idproduk,mp.namaPanjang,z.namadepartement,Y.name				
		) AS m
		GROUP BY 
		m.iddc,m.idproduk,m.kodeproduk,m.Barcode3,m.namaPanjang,m.Category,m.COA,m.idjenisstok
	) AS t 
	GROUP BY 
		t.iddc,t.kodeproduk,t.namaPanjang,t.Category,t.coa
	ORDER BY t.COA asc
--END;
