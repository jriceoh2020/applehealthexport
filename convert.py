#!/usr/bin/env python3
"""Convert Apple Health XML export to CSV files."""

import argparse
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# Maps HK identifiers to human-friendly filenames
TYPE_NAMES = {
    "HKQuantityTypeIdentifierHeartRate": "heart_rate",
    "HKQuantityTypeIdentifierStepCount": "steps",
    "HKQuantityTypeIdentifierBloodGlucose": "blood_glucose",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "active_energy",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "basal_energy",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "distance_walking_running",
    "HKQuantityTypeIdentifierDistanceCycling": "distance_cycling",
    "HKQuantityTypeIdentifierOxygenSaturation": "oxygen_saturation",
    "HKQuantityTypeIdentifierBloodPressureSystolic": "blood_pressure_systolic",
    "HKQuantityTypeIdentifierBloodPressureDiastolic": "blood_pressure_diastolic",
    "HKQuantityTypeIdentifierFlightsClimbed": "flights_climbed",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "heart_rate_variability",
    "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage": "walking_heart_rate_avg",
    "HKQuantityTypeIdentifierVO2Max": "vo2_max",
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratory_rate",
    "HKQuantityTypeIdentifierBodyMass": "body_mass",
    "HKQuantityTypeIdentifierHeight": "height",
    "HKQuantityTypeIdentifierPhysicalEffort": "physical_effort",
    "HKQuantityTypeIdentifierEnvironmentalAudioExposure": "environmental_audio",
    "HKQuantityTypeIdentifierWalkingSpeed": "walking_speed",
    "HKQuantityTypeIdentifierWalkingStepLength": "walking_step_length",
    "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage": "walking_double_support",
    "HKQuantityTypeIdentifierWalkingAsymmetryPercentage": "walking_asymmetry",
    "HKQuantityTypeIdentifierAppleWalkingSteadiness": "walking_steadiness",
    "HKQuantityTypeIdentifierAppleStandTime": "stand_time",
    "HKQuantityTypeIdentifierAppleExerciseTime": "exercise_time",
    "HKQuantityTypeIdentifierStairAscentSpeed": "stair_ascent_speed",
    "HKQuantityTypeIdentifierStairDescentSpeed": "stair_descent_speed",
    "HKQuantityTypeIdentifierTimeInDaylight": "time_in_daylight",
    "HKQuantityTypeIdentifierSixMinuteWalkTestDistance": "six_minute_walk_distance",
    "HKQuantityTypeIdentifierHeadphoneAudioExposure": "headphone_audio",
    "HKCategoryTypeIdentifierSleepAnalysis": "sleep_analysis",
    "HKCategoryTypeIdentifierAppleStandHour": "stand_hours",
    "HKCategoryTypeIdentifierAudioExposureEvent": "audio_exposure_events",
    "HKDataTypeSleepDurationGoal": "sleep_duration_goal",
}

# Blood pressure types are handled via Correlations, skip as top-level records
BP_TYPES = {
    "HKQuantityTypeIdentifierBloodPressureSystolic",
    "HKQuantityTypeIdentifierBloodPressureDiastolic",
}

WORKOUT_ACTIVITY_PREFIX = "HKWorkoutActivityType"


def friendly_name(hk_type):
    """Convert an HK identifier to a snake_case filename."""
    if hk_type in TYPE_NAMES:
        return TYPE_NAMES[hk_type]
    # Strip common prefixes and convert to snake_case
    name = hk_type
    for prefix in ("HKQuantityTypeIdentifier", "HKCategoryTypeIdentifier",
                    "HKDataType"):
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    # CamelCase to snake_case
    name = re.sub(r"([A-Z])", r"_\1", name).lower().lstrip("_")
    # Collapse double underscores
    name = re.sub(r"_+", "_", name)
    return name


def clean_date(date_str):
    """Strip timezone offset from date string for readability."""
    if not date_str:
        return ""
    # "2025-11-28 13:16:43 -0500" -> "2025-11-28 13:16:43"
    return re.sub(r"\s+[+-]\d{4}$", "", date_str)


def clean_workout_type(activity_type):
    """HKWorkoutActivityTypeWalking -> Walking"""
    if activity_type.startswith(WORKOUT_ACTIVITY_PREFIX):
        return activity_type[len(WORKOUT_ACTIVITY_PREFIX):]
    return activity_type


def clean_bio_sex(val):
    """HKBiologicalSexMale -> Male"""
    return val.replace("HKBiologicalSex", "") if val else ""


def clean_blood_type(val):
    """HKBloodTypeNotSet -> Not Set"""
    name = val.replace("HKBloodType", "") if val else ""
    # Insert spaces before capitals: "NotSet" -> "Not Set"
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", name)


def clean_skin_type(val):
    """HKFitzpatrickSkinTypeNotSet -> Not Set"""
    name = val.replace("HKFitzpatrickSkinType", "") if val else ""
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", name)


def parse_and_convert(input_dir, output_dir):
    export_path = os.path.join(input_dir, "export.xml")
    if not os.path.isfile(export_path):
        print(f"Error: {export_path} not found.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    # Buckets for records keyed by friendly name
    records = defaultdict(list)
    workouts = []
    activity_summaries = []
    correlations = []
    profile = None

    print(f"Parsing {export_path}...")

    # Track whether we're inside a Correlation element
    in_correlation = False
    current_correlation = None

    for event, elem in ET.iterparse(export_path, events=("start", "end")):
        tag = elem.tag

        if event == "start":
            if tag == "Correlation":
                in_correlation = True
                current_correlation = {
                    "type": elem.get("type", ""),
                    "sourceName": elem.get("sourceName", ""),
                    "creationDate": elem.get("creationDate", ""),
                    "startDate": elem.get("startDate", ""),
                    "endDate": elem.get("endDate", ""),
                    "records": [],
                }
            continue

        # event == "end" from here
        if tag == "Me":
            profile = {
                "date_of_birth": elem.get("HKCharacteristicTypeIdentifierDateOfBirth", ""),
                "biological_sex": clean_bio_sex(
                    elem.get("HKCharacteristicTypeIdentifierBiologicalSex", "")),
                "blood_type": clean_blood_type(
                    elem.get("HKCharacteristicTypeIdentifierBloodType", "")),
                "skin_type": clean_skin_type(
                    elem.get("HKCharacteristicTypeIdentifierFitzpatrickSkinType", "")),
            }

        elif tag == "Record":
            rec_type = elem.get("type", "")
            row = {
                "source": elem.get("sourceName", ""),
                "value": elem.get("value", ""),
                "unit": elem.get("unit", ""),
                "start_date": clean_date(elem.get("startDate", "")),
                "end_date": clean_date(elem.get("endDate", "")),
                "creation_date": clean_date(elem.get("creationDate", "")),
            }

            if in_correlation and current_correlation is not None:
                # Child record of a Correlation
                current_correlation["records"].append(
                    {"type": rec_type, **row})
            elif rec_type not in BP_TYPES:
                # Top-level record (skip BP types -- they're dupes of
                # Correlation children per the DTD comment)
                name = friendly_name(rec_type)
                records[name].append(row)

        elif tag == "Correlation":
            if current_correlation is not None:
                correlations.append(current_correlation)
            in_correlation = False
            current_correlation = None

        elif tag == "Workout":
            workouts.append({
                "activity_type": clean_workout_type(
                    elem.get("workoutActivityType", "")),
                "duration": elem.get("duration", ""),
                "duration_unit": elem.get("durationUnit", ""),
                "total_distance": elem.get("totalDistance", ""),
                "distance_unit": elem.get("totalDistanceUnit", ""),
                "total_energy_burned": elem.get("totalEnergyBurned", ""),
                "energy_unit": elem.get("totalEnergyBurnedUnit", ""),
                "source": elem.get("sourceName", ""),
                "start_date": clean_date(elem.get("startDate", "")),
                "end_date": clean_date(elem.get("endDate", "")),
            })

        elif tag == "ActivitySummary":
            activity_summaries.append({
                "date": elem.get("dateComponents", ""),
                "active_energy_burned": elem.get("activeEnergyBurned", ""),
                "active_energy_burned_goal": elem.get("activeEnergyBurnedGoal", ""),
                "exercise_time": elem.get("appleExerciseTime", ""),
                "exercise_time_goal": elem.get("appleExerciseTimeGoal", ""),
                "stand_hours": elem.get("appleStandHours", ""),
                "stand_hours_goal": elem.get("appleStandHoursGoal", ""),
            })

        # Free memory for elements we're done with
        elem.clear()

    # Print summary
    for name in sorted(records):
        print(f"  Found {len(records[name]):,} {name} records")
    if workouts:
        print(f"  Found {len(workouts):,} workouts")
    if activity_summaries:
        print(f"  Found {len(activity_summaries):,} activity summaries")
    if correlations:
        print(f"  Found {len(correlations):,} blood pressure correlations")

    file_count = 0

    # Write record CSVs
    record_fields = ["source", "value", "unit", "start_date", "end_date",
                     "creation_date"]
    for name in sorted(records):
        rows = records[name]
        path = os.path.join(output_dir, f"{name}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=record_fields)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Writing {name}.csv ({len(rows):,} records)")
        file_count += 1

    # Write blood pressure CSV from correlations
    if correlations:
        bp_rows = []
        for corr in correlations:
            systolic = ""
            diastolic = ""
            unit = ""
            for rec in corr["records"]:
                if "Systolic" in rec["type"]:
                    systolic = rec["value"]
                    unit = rec["unit"]
                elif "Diastolic" in rec["type"]:
                    diastolic = rec["value"]
            bp_rows.append({
                "systolic": systolic,
                "diastolic": diastolic,
                "unit": unit,
                "source": corr["sourceName"],
                "start_date": clean_date(corr["startDate"]),
                "end_date": clean_date(corr["endDate"]),
            })
        bp_fields = ["systolic", "diastolic", "unit", "source", "start_date",
                     "end_date"]
        path = os.path.join(output_dir, "blood_pressure.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=bp_fields)
            writer.writeheader()
            writer.writerows(bp_rows)
        print(f"  Writing blood_pressure.csv ({len(bp_rows):,} records)")
        file_count += 1

    # Write workouts CSV
    if workouts:
        workout_fields = ["activity_type", "duration", "duration_unit",
                          "total_distance", "distance_unit",
                          "total_energy_burned", "energy_unit",
                          "source", "start_date", "end_date"]
        path = os.path.join(output_dir, "workouts.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=workout_fields)
            writer.writeheader()
            writer.writerows(workouts)
        print(f"  Writing workouts.csv ({len(workouts):,} records)")
        file_count += 1

    # Write activity summary CSV
    if activity_summaries:
        summary_fields = ["date", "active_energy_burned",
                          "active_energy_burned_goal", "exercise_time",
                          "exercise_time_goal", "stand_hours",
                          "stand_hours_goal"]
        path = os.path.join(output_dir, "activity_summary.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=summary_fields)
            writer.writeheader()
            writer.writerows(activity_summaries)
        print(f"  Writing activity_summary.csv ({len(activity_summaries):,} records)")
        file_count += 1

    # Write profile CSV
    if profile:
        profile_fields = ["date_of_birth", "biological_sex", "blood_type",
                          "skin_type"]
        path = os.path.join(output_dir, "profile.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=profile_fields)
            writer.writeheader()
            writer.writerow(profile)
        print(f"  Writing profile.csv")
        file_count += 1

    print(f"\nDone! {file_count} CSV files written to {output_dir}/")


def main():
    parser = argparse.ArgumentParser(
        description="Convert Apple Health XML export to CSV files.")
    parser.add_argument("input_dir",
                        help="Path to exported Apple Health data folder "
                             "(containing export.xml)")
    parser.add_argument("output_dir",
                        help="Directory where CSV files will be written")
    args = parser.parse_args()
    parse_and_convert(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
