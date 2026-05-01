# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import itertools
from typing import Iterable

from sidra_fetcher.agregados import (
    Categoria,
    Classificacao,
    Variavel,
)


def unnest_dimensoes(
    variaveis: list[Variavel],
    classificacoes: list[Classificacao],
) -> Iterable[dict]:
    """Expand variables × classification categories into flat Dimensao rows.

    For each variable, computes the cartesian product of all categories
    across every classification (up to 6, mapped to d4–d9).  The unit of
    measure (``mc``/``mn``) is resolved with the following precedence:

    1. The category's own ``unidade`` field (when not ``None``).
    2. The variable's ``unidade`` field as a fallback.

    Args:
        variaveis: Iterable of :class:`~sidra_fetcher.agregados.Variavel`.
        classificacoes: Iterable of
            :class:`~sidra_fetcher.agregados.Classificacao`.

    Returns:
        A list of :class:`~sidra_sql.models.Dimensao` instances,
        one per (variavel, combination-of-categories) tuple.
    """

    # Pre-build a list of (categoria_list,) per classificacao so that
    # itertools.product can expand them correctly.
    cats_per_classificacao: list[list[Categoria]] = [
        classificacao.categorias for classificacao in classificacoes
    ]

    # Pad slots d4–d9: the model supports up to 6 classifications.
    MAX_CLASSIFICACOES = 6

    for variavel in variaveis:
        variavel_id = str(variavel.id)
        variavel_nome = variavel.nome
        unidade_id = None
        unidade_nome = variavel.unidade

        if not cats_per_classificacao:
            # No classifications: yield one row per variable with null d4–d9.
            yield dict(
                mc=unidade_id,
                mn=unidade_nome,
                d2c=variavel_id,
                d2n=variavel_nome,
                d4c=None,
                d4n=None,
                d5c=None,
                d5n=None,
                d6c=None,
                d6n=None,
                d7c=None,
                d7n=None,
                d8c=None,
                d8n=None,
                d9c=None,
                d9n=None,
            )
            continue

        # Cartesian product across all classifications.
        for combo in itertools.product(*cats_per_classificacao):
            # Resolve unit: first category that provides one wins;
            # fall back to the variable's own unit.
            for cat in combo:
                if cat.unidade is not None:
                    unidade_nome = cat.unidade
                    break

            # Map combo slots → d4…d9 (pad with None when fewer than 6).
            padded = list(combo) + [None] * (MAX_CLASSIFICACOES - len(combo))

            def _id(cat):
                return str(cat.id) if cat is not None else None

            def _nome(cat):
                return cat.nome if cat is not None else None

            yield dict(
                mc=unidade_id,
                mn=unidade_nome,
                d2c=variavel_id,
                d2n=variavel_nome,
                d4c=_id(padded[0]),
                d4n=_nome(padded[0]),
                d5c=_id(padded[1]),
                d5n=_nome(padded[1]),
                d6c=_id(padded[2]),
                d6n=_nome(padded[2]),
                d7c=_id(padded[3]),
                d7n=_nome(padded[3]),
                d8c=_id(padded[4]),
                d8n=_nome(padded[4]),
                d9c=_id(padded[5]),
                d9n=_nome(padded[5]),
            )
