"""Eventi opzionali di presentazione: non fanno parte degli artefatti misurati."""

from collections.abc import Callable
import warnings

ProgressCallback = Callable[[dict], None]


class Progress:
    """Isola gli errori dell'interfaccia: disabilita una callback guasta una volta.

    Non intercetta KeyboardInterrupt/SystemExit. Senza callback non produce
    output. Il contatore ingestione è un riferimento vivo, non una percentuale
    simulata: l'osservatore deve leggerlo senza modificarlo e senza bloccare.
    """

    def __init__(self, callback=None):
        self.callback = callback

    def __call__(self, **event):
        if self.callback is not None:
            try:
                self.callback(event)
            except Exception as error:
                self.callback = None
                warnings.warn(f"Interfaccia progresso disabilitata: {error}", RuntimeWarning, stacklevel=2)


def progress_sink(callback=None) -> Progress:
    """Condivide lo stesso isolamento fra runner e funzioni chiamate."""
    return callback if isinstance(callback, Progress) else Progress(callback)
