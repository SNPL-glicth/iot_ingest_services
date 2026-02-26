"""Authorization - Roles y permisos para ingesta universal (ISO 27001).

Define roles y estructura de permisos para control de acceso granular.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class Role(str, Enum):
    """Roles de autorización para ingesta de datos.
    
    ISO 27001 A.9.2.3 - Management of privileged access rights
    """
    ADMIN = "admin"  # Puede escribir en cualquier source_id y dominio
    SOURCE_WRITER = "source_writer"  # Solo escribe en su source_id asignado
    READ_ONLY = "read_only"  # Sin permisos de ingesta (solo consulta)


@dataclass
class ApiKeyInfo:
    """Información de autorización de un API key.
    
    Attributes:
        key_id: Identificador único del key (UUID)
        key_prefix: Primeros 8 caracteres del key (para logs)
        role: Rol asignado
        allowed_source_id: Source ID permitido (None = cualquiera, solo ADMIN)
        allowed_domains: Dominios permitidos ([] = todos, solo ADMIN)
        is_active: Si el key está activo
    """
    key_id: str
    key_prefix: str
    role: Role
    allowed_source_id: Optional[str]
    allowed_domains: List[str]
    is_active: bool
    
    def can_write_to_source(self, source_id: str) -> bool:
        """Verifica si puede escribir en el source_id dado.
        
        Args:
            source_id: Source ID a verificar
            
        Returns:
            True si tiene permiso, False si no
        """
        if self.role == Role.ADMIN:
            return True
        
        if self.role == Role.READ_ONLY:
            return False
        
        # SOURCE_WRITER: solo su source_id asignado
        return self.allowed_source_id == source_id
    
    def can_write_to_domain(self, domain: str) -> bool:
        """Verifica si puede escribir en el dominio dado.
        
        Args:
            domain: Dominio a verificar
            
        Returns:
            True si tiene permiso, False si no
        """
        if self.role == Role.ADMIN:
            return True
        
        if self.role == Role.READ_ONLY:
            return False
        
        # SOURCE_WRITER: verificar lista de dominios permitidos
        # Lista vacía = todos los dominios (solo ADMIN debería tener esto)
        if not self.allowed_domains:
            return self.role == Role.ADMIN
        
        return domain in self.allowed_domains
