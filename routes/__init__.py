# async def fetch_without_id(db, query, skip=0, limit=100):
#     """
#     Consulta datos excluyendo el campo `_id` para devolver todos los demás campos.
#     """
#     try:
#         collection = db.get_collection("genomas_vcf")
#         # Excluir solo el campo `_id`
#         cursor = collection.find(query, {"_id": 0}).skip(skip).limit(limit)
#         return await cursor.to_list(length=limit)
#     except Exception as e:
#         logger.error(f"Error fetching without _id: {e}")
#         return []


# async def execute_parallel_tasks_without_id(db, query, page_size, partitions):
#     """
#     Ejecuta consultas paralelas excluyendo el campo `_id`.
#     """
#     partition_size = page_size // partitions
#     tasks = [
#         fetch_without_id(db, query, skip=i * partition_size, limit=partition_size)
#         for i in range(partitions)
#     ]
#     results = await asyncio.gather(*tasks, return_exceptions=True)
#     return [
#         variant
#         for sublist in results
#         if isinstance(sublist, list)
#         for variant in sublist
#     ]


# @router.get("/variants/")
# async def get_variants_without_id(
#     field: str = Query(
#         ...,
#         description="Campo por el que se va a filtrar (CHROM, FILTER, INFO, FORMAT)",
#     ),
#     value: str = Query(..., description="Valor del campo para filtrar"),
#     page: int = Query(1, ge=1, description="Número de página (1 por defecto)"),
#     page_size: int = Query(
#         100, ge=1, le=1000, description="Tamaño de la página (100 por defecto)"
#     ),
# ):
#     """
#     Recupera variantes genómicas filtrando por un solo campo y valor,
#     excluyendo el campo `_id`.
#     """
#     try:
#         # Validar campo
#         if field not in VALID_FIELDS:
#             raise HTTPException(status_code=400, detail=f"Invalid field: {field}")

#         # Crear consulta dinámica
#         query = {field: value}

#         # Contar documentos totales
#         total_count = await db.get_collection("genomas_vcf").count_documents(query)
#         if total_count == 0:
#             return {
#                 "variants": [],
#                 "total_count": 0,
#                 "total_pages": 0,
#                 "current_page": 0,
#                 "page_size": page_size,
#                 "execution_time": "0 seconds",
#             }

#         # Calcular número total de páginas
#         total_pages = (total_count + page_size - 1) // page_size
#         if page > total_pages:
#             raise HTTPException(status_code=404, detail="Page out of range")

#         # Dividir en particiones
#         partitions = min(20, page_size // 100)  # Máximo 20 particiones
#         logger.info(
#             f"Query: {query}, Page: {page}, Page size: {page_size}, Partitions: {partitions}"
#         )

#         # Ejecutar consultas paralelas excluyendo `_id`
#         results = await execute_parallel_tasks_without_id(
#             db, query, page_size, partitions
#         )

#         # Responder
#         return {
#             "variants": results,
#             "total_count": total_count,
#             "total_pages": total_pages,
#             "current_page": page,
#             "page_size": page_size,
#         }

#     except HTTPException as e:
#         logger.error(f"HTTP Error in get_variants_without_id: {e.detail}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error in get_variants_without_id: {e}")
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
