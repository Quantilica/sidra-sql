# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

import datetime as dt

import sqlalchemy as sa
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    Identity,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SidraTabela(Base):
    __tablename__ = "sidra_tabela"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    periodicidade: Mapped[str] = mapped_column(Text, nullable=False)
    ultima_atualizacao: Mapped[Date] = mapped_column(
        Date,
        nullable=False,
        default=func.now(),
        onupdate=func.now(),
    )
    metadados: Mapped[JSONB] = mapped_column(JSONB, nullable=True)
    dados = relationship("Dados", back_populates="sidra_tabela")


class Localidade(Base):
    __tablename__ = "localidade"
    __table_args__ = (
        UniqueConstraint(
            "nc",
            "d1c",
            name="uq_localidade",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    dados = relationship("Dados", back_populates="localidade")
    # NC = NIVEL TERRITORIAL ID
    nc: Mapped[str] = mapped_column(Text, nullable=False)
    # NN = NIVEL TERRITORIAL NOME
    nn: Mapped[str] = mapped_column(Text, nullable=False)
    # D1C = UNIDADE TERRITORIAL ID
    d1c: Mapped[str] = mapped_column(Text, nullable=False)
    # D1N = UNIDADE TERRITORIAL NOME
    d1n: Mapped[str] = mapped_column(Text, nullable=False)


class Periodo(Base):
    __tablename__ = "periodo"
    __table_args__ = (
        UniqueConstraint(
            "codigo",
            "literals",
            name="uq_periodo",
            postgresql_nulls_not_distinct=True,
        ),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True), primary_key=True)
    codigo: Mapped[str] = mapped_column(Text, nullable=False)
    frequencia: Mapped[str | None] = mapped_column(Text)
    literals: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    data_inicio: Mapped[dt.date | None] = mapped_column(Date)
    data_fim: Mapped[dt.date | None] = mapped_column(Date)
    ano: Mapped[int | None] = mapped_column(Integer)
    ano_fim: Mapped[int | None] = mapped_column(Integer)
    semestre: Mapped[int | None] = mapped_column(
        SmallInteger,
        CheckConstraint("semestre IN (1, 2)"),
    )
    trimestre: Mapped[int | None] = mapped_column(
        SmallInteger,
        CheckConstraint("trimestre IN (1, 2, 3, 4)"),
    )
    mes: Mapped[int | None] = mapped_column(
        SmallInteger,
        CheckConstraint("mes IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"),
    )
    dados = relationship("Dados", back_populates="periodo")


class Dimensao(Base):
    __tablename__ = "dimensao"
    __table_args__ = (
        UniqueConstraint(
            "d2c",
            "d4c",
            "d5c",
            "d6c",
            "d7c",
            "d8c",
            "d9c",
            name="uq_dimensao",
            postgresql_nulls_not_distinct=True,
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    dados = relationship("Dados", back_populates="dimensao")
    # MC = UNIDADE ID
    mc: Mapped[str] = mapped_column(Text, nullable=True)
    # MN = UNIDADE NOME
    mn: Mapped[str] = mapped_column(Text, nullable=False)
    # D2C = VARIAVEL ID
    d2c: Mapped[str] = mapped_column(Text, nullable=False)
    # D2N = VARIAVEL NOME
    d2n: Mapped[str] = mapped_column(Text, nullable=False)
    # D4C = CATEGORIA ID da Classificação 1
    d4c: Mapped[str] = mapped_column(Text, nullable=True)
    # D4N = CATEGORIA NOME da Classificação 1
    d4n: Mapped[str] = mapped_column(Text, nullable=True)
    # D5C = CATEGORIA ID da Classificação 2
    d5c: Mapped[str] = mapped_column(Text, nullable=True)
    # D5N = CATEGORIA NOME da Classificação 2
    d5n: Mapped[str] = mapped_column(Text, nullable=True)
    # D6C = CATEGORIA ID da Classificação 3
    d6c: Mapped[str] = mapped_column(Text, nullable=True)
    # D6N = CATEGORIA NOME da Classificação 3
    d6n: Mapped[str] = mapped_column(Text, nullable=True)
    # D7C = CATEGORIA ID da Classificação 4
    d7c: Mapped[str] = mapped_column(Text, nullable=True)
    # D7N = CATEGORIA NOME da Classificação 4
    d7n: Mapped[str] = mapped_column(Text, nullable=True)
    # D8C = CATEGORIA ID da Classificação 5
    d8c: Mapped[str] = mapped_column(Text, nullable=True)
    # D8N = CATEGORIA NOME da Classificação 5
    d8n: Mapped[str] = mapped_column(Text, nullable=True)
    # D9C = CATEGORIA ID da Classificação 6
    d9c: Mapped[str] = mapped_column(Text, nullable=True)
    # D9N = CATEGORIA NOME da Classificação 6
    d9n: Mapped[str] = mapped_column(Text, nullable=True)


class Dados(Base):
    __tablename__ = "dados"
    __table_args__ = (
        sa.Index("ix_dados_periodo", "sidra_tabela_id", "periodo_id"),
        UniqueConstraint(
            "sidra_tabela_id",
            "localidade_id",
            "dimensao_id",
            "periodo_id",
            name="uq_dados",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    sidra_tabela_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_tabela.id"),
        nullable=False,
        index=True,
    )
    sidra_tabela = relationship("SidraTabela", back_populates="dados")
    dimensao_id: Mapped[int] = mapped_column(
        ForeignKey("dimensao.id"),
        nullable=False,
        index=True,
    )
    dimensao = relationship("Dimensao", back_populates="dados")
    localidade_id: Mapped[int] = mapped_column(
        ForeignKey("localidade.id"),
        nullable=False,
        index=True,
    )
    localidade = relationship("Localidade", back_populates="dados")
    periodo_id: Mapped[int] = mapped_column(
        ForeignKey("periodo.id"),
        nullable=False,
        index=True,
    )
    periodo = relationship("Periodo", back_populates="dados")
    modificacao: Mapped[Date] = mapped_column(Date, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # VALOR
    v: Mapped[str] = mapped_column(Text, nullable=False)
