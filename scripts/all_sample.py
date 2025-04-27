# subject=Virology
# subject=DesignPracticeManagement
# subject=Toxicology
# subject=GastroenterologyHepatology
# subject=BehavioralScienceComparativePsychology
# subject=EmergencyCriticalCareMedicine
# subject=Accounting

import os
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl  # 如果需要判断是否存在
from baselines.mappers.enrichers.language_id_enrichers import *
from baselines.mappers.filters.metadata_filters import *

import concurrent.futures

subjects_str = """
Acoustics
AerospaceAeronautics
AgronomyAgriculture
Allergy
AnalyticalChemistry
AnatomyMorphology
Anesthesiology
AppliedMathematics
AppliedPhysics
Architecture
ArthritisRheumatology
ArtificialIntelligenceImageProcessing
AstronomyAstrophysics
AutomobileDesignEngineering
BehavioralScienceComparativePsychology
BiochemistryMolecularBiology
Bioinformatics
BiomedicalEngineering
Biophysics
Biotechnology
BuildingConstruction
CardiovascularSystemHematology
ChemicalEngineering
ChemicalPhysics
CivilEngineering
ClinicalPsychology
ComplementaryAlternativeMedicine
ComputationTheoryMathematics
ComputerHardwareArchitecture
DairyAnimalScience
Dentistry
DermatologyVenerealDiseases
DesignPracticeManagement
DevelopmentalBiology
DevelopmentalChildPsychology
DistributedComputing
Ecology
ElectricalElectronicEngineering
EmergencyCriticalCareMedicine
EndocrinologyMetabolism
Energy
Entomology
EnvironmentalEngineering
EnvironmentalOccupationalHealth
EnvironmentalSciences
Epidemiology
EvolutionaryBiology
ExperimentalPsychology
Fisheries
FluidsPlasmas
FoodScience
Forestry
GastroenterologyHepatology
GeneralChemistry
GeneralClinicalMedicine
GeneralInternalMedicine
GeneralMathematics
GeneralPhysics
GeneralPsychologyCognitiveSciences
GeneticsHereditary
GeochemistryGeophysics
GeologicalGeomaticsEngineering
Geology
Geriatrics
Gerontology
HealthPolicyServices
Horticulture
HumanFactors
Immunology
IndustrialEngineeringAutomation
InformationSystems
InorganicNuclearChemistry
LegalForensicMedicine
MarineBiologyHydrobiology
Materials
MathematicalPhysics
MechanicalEngineeringTransports
MedicalInformatics
MedicinalBiomolecularChemistry
MeteorologyAtmosphericSciences
Microbiology
Microscopy
MiningMetallurgy
MycologyParasitology
NanoscienceNanotechnology
NetworkingTelecommunications
NeurologyNeurosurgery
NuclearMedicineMedicalImaging
NuclearParticlePhysics
NumericalComputationalMathematics
Nursing
NutritionDietetics
ObstetricsReproductiveMedicine
Oceanography
OncologyCarcinogenesis
OperationsResearch
OphthalmologyOptometry
Optics
OptoelectronicsPhotonics
OrganicChemistry
Ornithology
Orthopedics
Otorhinolaryngology
Paleontology
Pathology
Pediatrics
PharmacologyPharmacy
PhysicalChemistry
Physiology
PlantBiologyBotany
Polymers
Psychiatry
Psychoanalysis
PublicHealth
Rehabilitation
RespiratorySystem
SocialPsychology
SoftwareEngineering
SpeechLanguagePathologyAudiology
SportSciences
StatisticsProbability
StrategicDefenceSecurityStudies
SubstanceAbuse
Surgery
Toxicology
TropicalMedicine
UrbanRegionalPlanning
UrologyNephrology
VeterinarySciences
Virology
Zoology
"""

subjects_str = "AppliedMathematics"
subjects_str = "AppliedPhysics"
subjects_str = "OrganicChemistry"
subjects_strs = ["ArtificialIntelligenceImageProcessing",
                "EnvironmentalEngineering",
                "SoftwareEngineering",
                "Geology",
                "Economics",
                "Philosophy",
                "LiteraryStudies",
                "SocialPsychology",
                ]
subjects_strs = [
    "ElectricalElectronicEngineering",
    "Optics",
    "Finance",
    "Law",
]


subjects_strs = ["AnalyticalChemistry",
 "Allergy",
 "Anesthesiology",
 "AnatomyMorphology",
 "Acoustics",
 "Architecture",
 "ArtPracticeHistoryTheory",
 "ArthritisRheumatology",
 "Anthropology",
 "ArtificialIntelligenceImageProcessing",
 "AppliedMathematics",
 "BehavioralScienceComparativePsychology",
 "BiochemistryMolecularBiology",
 "AgriculturalEconomicsPolicy",
 "Archaeology",
 "Biophysics",
 "AerospaceAeronautics",
 "BuildingConstruction",
 "Accounting",
 "CardiovascularSystemHematology",
 "AutomobileDesignEngineering",
 "Bioinformatics",
 "ChemicalEngineering",
 "ChemicalPhysics",
 "ClinicalPsychology",
 "AstronomyAstrophysics",
 "ComplementaryAlternativeMedicine",
 "ComputationTheoryMathematics",
 "AppliedEthics",
 "ComputerHardwareArchitecture",
 "CommunicationMediaStudies",
 "BiomedicalEngineering",
 "AgronomyAgriculture",
 "AppliedPhysics",
 "DermatologyVenerealDiseases",
 "DesignPracticeManagement",
 "Dentistry",
 "DairyAnimalScience",
 "DevelopmentalChildPsychology",
 "DistributedComputing",
 "DevelopmentalBiology",
 "DramaTheater",
 "Classics",
 "EconomicTheory",
 "Econometrics",
 "Ecology",
 "Criminology",
 "EmergencyCriticalCareMedicine",
 "EndocrinologyMetabolism",
 "Education",
 "CivilEngineering",
 "Entomology",
 "EnvironmentalOccupationalHealth",
 "Demography",
 "Epidemiology",
 "ElectricalElectronicEngineering",
 "ExperimentalPsychology",
 "EvolutionaryBiology",
 "EnvironmentalEngineering",
 "Biotechnology",
 "CulturalStudies",
 "Folklore",
 "FluidsPlasmas",
 "FamilyStudies",
 "GastroenterologyHepatology",
 "Energy",
 "Finance",
 "GeneralChemistry",
 "GeneralClinicalMedicine",
 "GeneralInternalMedicine",
 "Fisheries",
 "GeneralPsychologyCognitiveSciences",
 "GeneticsHereditary",
 "FoodScience",
 "GeochemistryGeophysics",
 "GenderStudies",
 "Forestry",
 "Geriatrics",
 "EnvironmentalSciences",
 "Gerontology",
 "GeneralMathematics",
 "HistoryofScienceTechnologyMedicine",
 "History",
 "HealthPolicyServices",
 "HumanFactors",
 "GeologicalGeomaticsEngineering",
 "Immunology",
 "Geography",
 "HistoryofSocialSciences",
 "InformationSystems",
 "InorganicNuclearChemistry",
 "IndustrialEngineeringAutomation",
 "Geology",
 "IndustrialRelations",
 "LegalForensicMedicine",
 "LanguagesLinguistics",
 "Law",
 "MarineBiologyHydrobiology",
 "LogisticsTransportation",
 "InformationLibrarySciences",
 "MathematicalPhysics",
 "GeneralPhysics",
 "MedicalInformatics",
 "MedicinalBiomolecularChemistry",
 "Horticulture",
 "Microbiology",
 "Microscopy",
 "MeteorologyAtmosphericSciences",
 "Music",
 "MycologyParasitology",
 "MiningMetallurgy",
 "NetworkingTelecommunications",
 "NeurologyNeurosurgery",
 "NuclearMedicineMedicalImaging",
 "NanoscienceNanotechnology",
 "Marketing",
 "NumericalComputationalMathematics",
 "LiteraryStudies",
 "NutritionDietetics",
 "ObstetricsReproductiveMedicine",
 "OncologyCarcinogenesis",
 "NuclearParticlePhysics",
 "OphthalmologyOptometry",
 "OperationsResearch",
 "Materials",
 "OrganicChemistry",
 "InternationalRelations",
 "Orthopedics",
 "Otorhinolaryngology",
 "Optics",
 "Pathology",
 "Pediatrics",
 "PharmacologyPharmacy",
 "Nursing",
 "PhysicalChemistry",
 "Physiology",
 "Ornithology",
 "MechanicalEngineeringTransports",
 "Polymers",
 "Psychiatry",
 "Psychoanalysis",
 "OptoelectronicsPhotonics",
 "Oceanography",
 "DevelopmentStudies",
 "RespiratorySystem",
 "ScienceStudies",
 "SocialPsychology",
 "Paleontology",
 "SocialSciencesMethods",
 "Rehabilitation",
 "SoftwareEngineering",
 "PlantBiologyBotany",
 "PublicHealth",
 "SportSciences",
 "Philosophy",
 "SpeechLanguagePathologyAudiology",
 "SocialWork",
 "Surgery",
 "Toxicology",
 "TropicalMedicine",
 "SubstanceAbuse",
 "UrologyNephrology",
 "UrbanRegionalPlanning",
 "Virology",
 "BusinessManagement",
 "PoliticalSciencePublicAdministration",
 "SportLeisureTourism",
 "StatisticsProbability",
 "Zoology",
 "Sociology",
 "VeterinarySciences",
 "StrategicDefenceSecurityStudies",
 "Economics",
 "ReligionsTheology"]

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main(subject_str, lang='en'):
    oss_dir = "oss://train1/basemodel-subjet-data/r2/dclm/"
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)

    num = 20
    lines = []
    subject_paths = oss.get_sub_folders(bucket, path)

    lang_fn = detect_lang_whole_page_enricher(model='fasttext')
    
    for k, subject_path in enumerate(subject_paths):
        subject_name = subject_path.split("/")[-2].split("=")[-1]
        if subject_name not in subject_str:
            continue

        
        files = oss.get_sub_files(bucket, subject_path)
        f = files[0]
        f = oss.join_file_path(bucket_name, f)

        for i, line in enumerate(read_jsonl(f)):
            if len(line['text']) == 0:
                continue

            line = lang_fn(line)[0]
            rets = language_filter(line, keep_languages=[lang], key='language_id_whole_page_fasttext', threshold=0.65)
            if not rets:
                continue
            line = rets[0]
            lines.append(line)
            
            if len(lines) == num:
                break
        if len(lines) == num:
            break

    print(f"{subject_str} {lang} done {num}")
    output_folder = "oss://si002558te8h/dclm/origin/Experiment4_en/"    
    write_jsonl(lines, os.path.join(output_folder, f"{subject_str}_{lang}_{num}.jsonl"))

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for subject_str in subjects_strs:
            futures.append(executor.submit(main, subject_str, 'en'))

        for future in concurrent.futures.as_completed(futures):
            future.result()
