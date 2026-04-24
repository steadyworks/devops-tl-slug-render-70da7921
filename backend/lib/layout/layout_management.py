from abc import ABC, abstractmethod

from pydantic import BaseModel

from backend.db.data_models import PageLayout


class SectionRenderContext(BaseModel):
    num_of_images: int


class FitResult(BaseModel):
    ok: bool
    score: int
    reasons: list[str]


class SectionTemplate(ABC):

    @property
    @abstractmethod
    def id(self) -> PageLayout:
        """Unique ID for this template."""
        pass

    @abstractmethod
    def fits(self, ctx: SectionRenderContext) -> FitResult:
        raise NotImplementedError()


class SectionTemplateMasonry(SectionTemplate):
    @property
    def id(self) -> PageLayout:
        return PageLayout.MASONRY

    def fits(self, ctx: SectionRenderContext) -> FitResult:
        if ctx.num_of_images >= 3:
            return FitResult(ok=True, score=100, reasons=[])
        return FitResult(ok=False, score=0, reasons=["Not enough images"])


class SectionTemplatePolaroid(SectionTemplate):
    @property
    def id(self) -> PageLayout:
        return PageLayout.POLAROID

    def fits(self, ctx: SectionRenderContext) -> FitResult:
        return FitResult(
            ok=True, score=100, reasons=["Polaroid is a great choice!"]
        )


class SectionTemplateSurrounding(SectionTemplate):
    @property
    def id(self) -> PageLayout:
        return PageLayout.SURROUNDING

    def fits(self, ctx: SectionRenderContext) -> FitResult:
        if ctx.num_of_images >= 3 and ctx.num_of_images <= 6:
            return FitResult(ok=True, score=100, reasons=[])
        return FitResult(
            ok=False, score=0, reasons=["Too many or too few images"]
        )


class SectionTemplateTwoDiagonal(SectionTemplate):
    @property
    def id(self) -> PageLayout:
        return PageLayout.TWO_DIAGONAL

    def fits(self, ctx: SectionRenderContext) -> FitResult:
        if ctx.num_of_images == 2:
            return FitResult(ok=True, score=100, reasons=[])
        return FitResult(
            ok=False, score=0, reasons=["Invalid number of images"]
        )


def get_all_layout_options() -> list[SectionTemplate]:
    return [
        SectionTemplateMasonry(),
        SectionTemplatePolaroid(),
        SectionTemplateSurrounding(),
        SectionTemplateTwoDiagonal(),
    ]
