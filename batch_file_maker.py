def func(_start, _end, _parts):
    elements_per_part = (_end - _start + 1) // _parts
    _remainder = (_end - _start + 1) % _parts
    _current = _start

    with open('run.bat', 'w') as file:
        for part in range(1, _parts + 1):
            start_id = _current
            if part <= _remainder:
                end_id = start_id + elements_per_part
            else:
                end_id = start_id + elements_per_part - 1
            _current = end_id + 1

            file.write(f'start python -m data_saver {start_id} {end_id}\n')


func(_start=1, _end=418, _parts=10)
