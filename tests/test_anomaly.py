def is_energy_spike(energy_kw: float, threshold: float = 200) -> bool:
    return energy_kw > threshold

def test_spike_detection():
    assert is_energy_spike(250)
    assert not is_energy_spike(150)
