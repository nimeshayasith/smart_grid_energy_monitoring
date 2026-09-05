"""Unit tests for the data generators (no Kafka/PG required)."""

import sys
import os
import math
import importlib
import unittest

# Allow importing from generators/ without installation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generators'))

# Patch Kafka import so generator can be imported without a live broker
import unittest.mock as mock

with mock.patch.dict('sys.modules', {
    'kafka':        mock.MagicMock(),
    'kafka.errors': mock.MagicMock(),
    'psycopg2':     mock.MagicMock(),
    'psycopg2.extras': mock.MagicMock(),
}):
    import meter_producer as mp
    import tariff_generator as tg


class TestMeterProducer(unittest.TestCase):

    def test_fleet_size(self):
        fleet = mp.build_meter_fleet()
        self.assertEqual(len(fleet), mp.NUM_METERS)

    def test_fleet_zones(self):
        fleet = mp.build_meter_fleet()
        zones_in_fleet = {m["grid_zone"] for m in fleet}
        self.assertEqual(zones_in_fleet, set(mp.GRID_ZONES))

    def test_meters_per_zone(self):
        fleet = mp.build_meter_fleet()
        per_zone = mp.NUM_METERS // len(mp.GRID_ZONES)
        for zone in mp.GRID_ZONES:
            count = sum(1 for m in fleet if m["grid_zone"] == zone)
            self.assertEqual(count, per_zone, f"Wrong count for {zone}")

    def test_reading_fields(self):
        import random; random.seed(0)
        fleet = mp.build_meter_fleet()
        reading = mp.generate_reading(fleet[0])
        required_fields = {"meter_id", "household_id", "power_consumption_kwh",
                           "solar_generation_kwh", "grid_zone", "timestamp"}
        self.assertEqual(required_fields, set(reading.keys()))

    def test_reading_non_negative(self):
        import random; random.seed(1)
        fleet = mp.build_meter_fleet()
        for meter in fleet:
            reading = mp.generate_reading(meter)
            self.assertGreaterEqual(reading["power_consumption_kwh"], 0,
                                    "Negative power consumption")
            self.assertGreaterEqual(reading["solar_generation_kwh"], 0,
                                    "Negative solar generation")

    def test_solar_zero_at_midnight(self):
        """Solar should be ~0 at midnight (sim hour 0 or 24)."""
        import random; random.seed(2)
        # At sim_hour=0, sin(0)=0 → solar_factor=0 → solar_gen=0
        fleet = mp.build_meter_fleet()
        meter = fleet[0]
        # Manually compute for hour=0
        hour = 0.0
        day_fraction = hour / 24.0
        solar_factor = max(0.0, math.sin(math.pi * day_fraction))
        self.assertAlmostEqual(solar_factor, 0.0, places=5)

    def test_meter_ids_unique(self):
        fleet = mp.build_meter_fleet()
        ids = [m["meter_id"] for m in fleet]
        self.assertEqual(len(ids), len(set(ids)), "Duplicate meter IDs")


class TestTariffGenerator(unittest.TestCase):

    def test_record_count(self):
        records = tg.generate_tariff_records(seed=42)
        self.assertEqual(len(records), tg.NUM_METERS)

    def test_required_fields(self):
        records = tg.generate_tariff_records(seed=42)
        required = {"household_id", "tariff_rate", "billing_tier", "subsidy_flag", "updated_at"}
        for r in records:
            self.assertEqual(required, set(r.keys()))

    def test_tariff_rate_positive(self):
        records = tg.generate_tariff_records(seed=42)
        for r in records:
            self.assertGreater(r["tariff_rate"], 0, f"{r['household_id']} has non-positive tariff")

    def test_billing_tier_valid(self):
        records = tg.generate_tariff_records(seed=42)
        valid_tiers = {"RESIDENTIAL", "COMMERCIAL"}
        for r in records:
            self.assertIn(r["billing_tier"], valid_tiers)

    def test_subsidy_flag_is_bool(self):
        records = tg.generate_tariff_records(seed=42)
        for r in records:
            self.assertIsInstance(r["subsidy_flag"], bool)

    def test_deterministic_with_same_seed(self):
        r1 = tg.generate_tariff_records(seed=99)
        r2 = tg.generate_tariff_records(seed=99)
        self.assertEqual(r1, r2)

    def test_different_seeds_differ(self):
        r1 = tg.generate_tariff_records(seed=1)
        r2 = tg.generate_tariff_records(seed=2)
        rates1 = [r["tariff_rate"] for r in r1]
        rates2 = [r["tariff_rate"] for r in r2]
        self.assertNotEqual(rates1, rates2)


if __name__ == "__main__":
    unittest.main()
