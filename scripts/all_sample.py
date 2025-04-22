# subject=Virology
# subject=DesignPracticeManagement
# subject=Toxicology
# subject=GastroenterologyHepatology
# subject=BehavioralScienceComparativePsychology
# subject=EmergencyCriticalCareMedicine
# subject=Accounting

import time
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl  # 如果需要判断是否存在

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

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main(subject_str):
    oss_dir = "oss://train1/basemodel-subjet-data/r2/dclm/"
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)

    num = 1000
    lines = []
    subject_paths = oss.get_sub_folders(bucket, path)
    for k, subject_path in enumerate(subject_paths):
        subject_name = subject_path.split("/")[-2].split("=")[-1]
        if subject_name not in subject_str:
            continue

        print(k)
        files = oss.get_sub_files(bucket, subject_path)
        f = files[0]
        f = oss.join_file_path(bucket_name, f)
        for i, line in enumerate(read_jsonl(f)):
            lines.append(line)
            if i == num-1:
                break

    write_jsonl(lines, f"./{subject_str}_1000.jsonl")

if __name__ == '__main__':
    for subject_str in subjects_strs:
        main(subject_str)
