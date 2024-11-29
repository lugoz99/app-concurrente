from typing import List, Optional
from fastapi import APIRouter, Depends, UploadFile
from schemas.genoma_schema import VCFEntryCreate
from services.genoma_controller import VCFController, VCFService
from database.dependencies import get_database

router = APIRouter()


@router.post("/upload", response_model=dict, status_code=200)
async def upload_vcf(file: UploadFile, db=Depends(get_database)):
    controller = VCFController(db)
    result = await controller.process_vcf(file)
    return {"message": "VCF file processed successfully", "data": result}


@router.get("/search_vcf", response_model=List[VCFEntryCreate])
async def search_vcf(
    chrom: Optional[str] = None,
    filter: Optional[str] = None,
    info: Optional[str] = None,
    format: Optional[str] = None,
    db=Depends(get_database),
):
    service = VCFService(db)
    entries = await service.search_vcf_entries(chrom, filter, info, format)
    return entries
