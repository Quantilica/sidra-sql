# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import tomllib
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class Severity(Enum):
    OK = "ok"
    WARN = "warn"
    ERROR = "error"


@dataclass
class Issue:
    severity: Severity
    message: str


@dataclass
class SectionReport:
    title: str
    issues: list[Issue] = field(default_factory=list)

    def ok(self, msg: str) -> None:
        self.issues.append(Issue(Severity.OK, msg))

    def warn(self, msg: str) -> None:
        self.issues.append(Issue(Severity.WARN, msg))

    def error(self, msg: str) -> None:
        self.issues.append(Issue(Severity.ERROR, msg))

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.WARN]


@dataclass
class ValidationReport:
    sections: list[SectionReport] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(len(s.errors) for s in self.sections)

    @property
    def warning_count(self) -> int:
        return sum(len(s.warnings) for s in self.sections)

    @property
    def is_valid(self) -> bool:
        return self.error_count == 0


class PluginValidator:
    def __init__(self, plugin_dir: Path):
        self.plugin_dir = plugin_dir

    def validate(self) -> ValidationReport:
        report = ValidationReport()

        manifest_section = SectionReport("manifest.toml")
        report.sections.append(manifest_section)

        manifest_path = self.plugin_dir / "manifest.toml"
        if not manifest_path.exists():
            manifest_section.error("manifest.toml não encontrado")
            return report

        try:
            with open(manifest_path, "rb") as f:
                manifest = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            manifest_section.error(f"TOML inválido: {e}")
            return report

        manifest_section.ok("TOML válido")

        if "name" not in manifest:
            manifest_section.warn("Campo 'name' ausente")
        if "version" not in manifest:
            manifest_section.warn("Campo 'version' ausente")

        pipelines = manifest.get("pipeline", [])
        if not pipelines:
            manifest_section.warn("Nenhum [[pipeline]] declarado")
        else:
            manifest_section.ok(f"{len(pipelines)} pipeline(s) declarado(s)")

        ids_seen: set[str] = set()
        valid_pipelines: list[dict] = []

        for i, p in enumerate(pipelines):
            entry = f"pipeline[{i}]"
            pid = p.get("id")
            ppath = p.get("path")

            if not pid:
                manifest_section.error(f"{entry}: campo 'id' ausente")
                continue
            if not ppath:
                manifest_section.error(f"pipeline '{pid}': campo 'path' ausente")
                continue
            if pid in ids_seen:
                manifest_section.error(f"ID duplicado: '{pid}'")
                continue

            ids_seen.add(pid)
            valid_pipelines.append(p)

        for p in valid_pipelines:
            section = SectionReport(p["path"])
            report.sections.append(section)
            self._validate_pipeline(p["id"], p["path"], section)

        return report

    def _validate_pipeline(self, pid: str, rel_path: str, section: SectionReport) -> None:
        pipeline_dir = self.plugin_dir / rel_path

        if not pipeline_dir.exists():
            section.error(f"Diretório não encontrado: '{pipeline_dir}'")
            return

        has_fetch = (pipeline_dir / "fetch.toml").exists()
        has_transform = (pipeline_dir / "transform.toml").exists()

        if not has_fetch and not has_transform:
            section.error("Nenhum fetch.toml ou transform.toml encontrado")
            return

        if has_fetch:
            self._validate_fetch_toml(pipeline_dir, section)

        if has_transform:
            self._validate_transform_toml(pipeline_dir, section)
            if not (pipeline_dir / "transform.sql").exists():
                section.error("transform.toml presente mas transform.sql ausente")
            else:
                section.ok("transform.sql presente")

    def _validate_fetch_toml(self, pipeline_dir: Path, section: SectionReport) -> None:
        fetch_path = pipeline_dir / "fetch.toml"
        try:
            with open(fetch_path, "rb") as f:
                data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            section.error(f"fetch.toml: TOML inválido: {e}")
            return

        tabelas = data.get("tabelas", [])
        if not tabelas:
            section.error("fetch.toml: nenhuma [[tabelas]] declarada")
            return

        for i, t in enumerate(tabelas):
            if "sidra_tabela" not in t:
                section.error(f"fetch.toml: tabelas[{i}] sem campo 'sidra_tabela'")

        section.ok(f"fetch.toml válido ({len(tabelas)} tabela(s))")

    def _validate_transform_toml(self, pipeline_dir: Path, section: SectionReport) -> None:
        transform_path = pipeline_dir / "transform.toml"
        try:
            with open(transform_path, "rb") as f:
                data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            section.error(f"transform.toml: TOML inválido: {e}")
            return

        table = data.get("table")
        if not table:
            section.error("transform.toml: seção [table] ausente")
            return

        missing = [f for f in ("name", "schema", "strategy") if f not in table]
        if missing:
            section.error(
                f"transform.toml: campo(s) obrigatório(s) ausente(s) em [table]: {', '.join(missing)}"
            )
        else:
            section.ok("transform.toml válido")
