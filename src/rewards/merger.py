"""Position merger on-chain via NegRiskAdapter.

Combina YES + NO conditional tokens de vuelta a pUSD (collateral)
llamando a mergePositions() en el contrato NegRiskAdapter de Polymarket
desplegado en Polygon (0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296).

Flujo:
  1. Verificar que la wallet tiene ambos tokens (YES y NO)
  2. Aprobar al NegRiskAdapter para gastar los conditional tokens
  3. Llamar mergePositions(conditionId, amount)
  4. El contrato quema YES+NO y devuelve collateral (pUSD)

Referencia: PROMPT_REWARDS_FARMING.md paso 4
"""

import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("nachomarket.rewards.merger")

# Contratos en Polygon mainnet (chain_id=137)
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
COLLATERAL_TOKEN = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # pUSD

# ABIs minimos
NEG_RISK_ADAPTER_ABI = [
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

CTF_ABI = [
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "id", "type": "uint256"},
            {"name": "account", "type": "address"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# RPC endpoints (fallback order)
POLYGON_RPCS = [
    os.environ.get("POLYGON_RPC_URL", ""),
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://rpc.ankr.com/polygon",
]


class PositionMerger:
    """Merge on-chain de posiciones YES+NO via NegRiskAdapter.

    Args:
        private_key: Private key de la wallet (de .env POLYMARKET_PRIVATE_KEY).
        rpc_url: URL del RPC de Polygon (opcional, usa fallbacks).
    """

    def __init__(
        self,
        private_key: str | None = None,
        rpc_url: str | None = None,
        paper_mode: bool = True,
    ) -> None:
        self._paper_mode = paper_mode
        self._w3: Any = None
        self._account: Any = None
        self._adapter: Any = None
        self._ctf: Any = None
        self._initialized = False

        if paper_mode:
            logger.info("PositionMerger: modo paper, merge on-chain deshabilitado")
            return

        try:
            from web3 import Web3

            # Conectar a Polygon
            rpcs = [r for r in [rpc_url] + POLYGON_RPCS if r]
            for rpc in rpcs:
                try:
                    self._w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                    if self._w3.is_connected():
                        logger.info("PositionMerger: conectado a %s", rpc[:50])
                        break
                    self._w3 = None
                except Exception:
                    self._w3 = None
                    continue

            if not self._w3 or not self._w3.is_connected():
                logger.error("PositionMerger: no se pudo conectar a Polygon RPC")
                return

            # Configurar cuenta
            if not private_key:
                logger.error("PositionMerger: private_key no proporcionada")
                return
            self._account = self._w3.eth.account.from_key(private_key)
            logger.info(
                "PositionMerger: wallet=%s",
                self._account.address[:10] + "...",
            )

            # Cargar contratos
            self._adapter = self._w3.eth.contract(
                address=Web3.to_checksum_address(NEG_RISK_ADAPTER),
                abi=NEG_RISK_ADAPTER_ABI,
            )
            self._ctf = self._w3.eth.contract(
                address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
                abi=CTF_ABI,
            )

            self._initialized = True
            logger.info("PositionMerger: inicializado correctamente")

        except ImportError:
            logger.error("PositionMerger: web3 no instalado. pip install web3")
        except Exception:
            logger.exception("PositionMermer: error en inicializacion")

    @property
    def is_ready(self) -> bool:
        """True si el merger puede ejecutar merges on-chain."""
        return self._initialized and not self._paper_mode

    def merge_positions(self, condition_id: str, amount: float) -> dict[str, Any]:
        """Merge YES+NO shares de vuelta a collateral (pUSD).

        Args:
            condition_id: Condition ID del mercado (hex string, 0x...).
            amount: Cantidad de shares a mergear (en unidades humanas, no wei).

        Returns:
            Dict con status, tx_hash, amount_merged.
        """
        if self._paper_mode:
            logger.info(
                "[PAPER] merge_positions: %s shares en %s...",
                amount, condition_id[:12],
            )
            return {"status": "paper", "amount": amount, "tx_hash": ""}

        if not self.is_ready:
            logger.error("merge_positions: merger no inicializado")
            return {"status": "error", "reason": "not_initialized"}

        try:
            from web3 import Web3

            # Convertir condition_id a bytes32
            if condition_id.startswith("0x"):
                cond_bytes = bytes.fromhex(condition_id[2:])
            else:
                cond_bytes = bytes.fromhex(condition_id)
            cond_bytes32 = cond_bytes.rjust(32, b"\x00")

            # Amount en unidades de conditional token (6 decimales)
            amount_wei = int(amount * 10**6)

            # 1. Verificar approval
            is_approved = self._ctf.functions.isApprovedForAll(
                self._account.address,
                Web3.to_checksum_address(NEG_RISK_ADAPTER),
            ).call()

            if not is_approved:
                logger.info("PositionMerger: aprobando NegRiskAdapter para CTF...")
                tx = self._ctf.functions.setApprovalForAll(
                    Web3.to_checksum_address(NEG_RISK_ADAPTER),
                    True,
                ).build_transaction({
                    "from": self._account.address,
                    "nonce": self._w3.eth.get_transaction_count(self._account.address),
                    "gas": 100000,
                    "gasPrice": self._w3.eth.gas_price,
                })
                signed = self._w3.eth.account.sign_transaction(tx, self._account.key)
                tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                if receipt["status"] != 1:
                    logger.error("PositionMerger: approval falló")
                    return {"status": "error", "reason": "approval_failed"}
                logger.info("PositionMerger: approval OK (tx=%s)", tx_hash.hex()[:16])

            # 2. Ejecutar merge
            tx = self._adapter.functions.mergePositions(
                cond_bytes32,
                amount_wei,
            ).build_transaction({
                "from": self._account.address,
                "nonce": self._w3.eth.get_transaction_count(self._account.address),
                "gas": 300000,
                "gasPrice": self._w3.eth.gas_price,
            })
            signed = self._w3.eth.account.sign_transaction(tx, self._account.key)
            tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

            if receipt["status"] == 1:
                logger.info(
                    "PositionMerger: merge OK — %s shares -> pUSD (tx=%s)",
                    amount, tx_hash.hex()[:16],
                )
                return {
                    "status": "success",
                    "tx_hash": tx_hash.hex(),
                    "amount": amount,
                    "gas_used": receipt.get("gasUsed", 0),
                }
            else:
                logger.error("PositionMerger: merge falló (tx=%s)", tx_hash.hex()[:16])
                return {"status": "error", "reason": "tx_reverted", "tx_hash": tx_hash.hex()}

        except Exception:
            logger.exception("PositionMerger: error en merge_positions")
            return {"status": "error", "reason": "exception"}

    def get_token_balance(self, token_id: str) -> float:
        """Obtiene el balance de un conditional token.

        Args:
            token_id: Token ID del conditional token (hex string).

        Returns:
            Balance en unidades humanas (no wei).
        """
        if not self.is_ready or not self._ctf:
            return 0.0

        try:
            token_id_int = int(token_id, 16) if isinstance(token_id, str) else token_id
            balance = self._ctf.functions.balanceOf(
                self._account.address,
                token_id_int,
            ).call()
            return balance / 10**6
        except Exception:
            logger.debug("get_token_balance: error para token %s...", str(token_id)[:8])
            return 0.0
