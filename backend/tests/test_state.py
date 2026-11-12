"""Tests unitarios del estado replicado. Ejecutar desde backend/: python -m pytest"""
import pytest

from chat.state import ChatState, StateError


def test_sala_general_por_defecto():
    assert ChatState().room_summary() == [{"name": "general", "messages": 0}]


def test_lider_y_replica_convergen():
    lider, replica = ChatState(), ChatState()
    ops = []
    for build in (
        lambda: lider.new_create_room_op("deportes"),
        lambda: lider.new_message_op("deportes", "ana", "hola", 1),
        lambda: lider.new_message_op("general", "beto", "buenas", 1),
    ):
        op = build()
        assert lider.apply(op) == "applied"
        ops.append(op)

    for op in ops:
        assert replica.apply(op) == "applied"
    assert replica.snapshot() == lider.snapshot()


def test_operacion_duplicada_se_ignora():
    s = ChatState()
    op = s.new_message_op("general", "ana", "hola", 1)
    assert s.apply(op) == "applied"
    assert s.apply(op) == "duplicate"
    assert len(s.history("general")) == 1


def test_hueco_en_el_log_se_detecta():
    lider, replica = ChatState(), ChatState()
    op1 = lider.new_message_op("general", "ana", "1", 1)
    lider.apply(op1)
    op2 = lider.new_message_op("general", "ana", "2", 1)
    assert replica.apply(op2) == "gap"
    assert replica.last_index == 0


def test_snapshot_restaura_estado():
    a = ChatState()
    a.apply(a.new_create_room_op("x"))
    b = ChatState()
    b.load_snapshot(a.snapshot())
    assert b.snapshot() == a.snapshot()


@pytest.mark.parametrize("nombre", ["", "   ", "x" * 33])
def test_nombre_de_sala_invalido(nombre):
    with pytest.raises(StateError):
        ChatState().new_create_room_op(nombre)


def test_sala_duplicada_o_inexistente():
    s = ChatState()
    with pytest.raises(StateError):
        s.new_create_room_op("general")
    with pytest.raises(StateError):
        s.new_message_op("no-existe", "ana", "hola", 1)
