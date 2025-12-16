"""Tests unitaires pour le module fetcher."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import httpx

from pipeline.fetcher import fetch_page, fetch_all_data
from pipeline.config import API_BASE_URL


class TestFetchPage:
    """Tests pour la fonction fetch_page."""
    
    @patch('pipeline.fetcher.httpx.Client')
    def test_fetch_page_success(self, mock_client_class):
        """Test d'une récupération réussie."""
        # Mock de la réponse
        mock_response = Mock()
        mock_response.json.return_value = {"products": [{"id": 1, "name": "Test"}]}
        mock_response.raise_for_status = Mock()
        
        # Mock du client
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        # Test
        result = fetch_page("/search", {"page": 1, "page_size": 10})
        
        assert result == {"products": [{"id": 1, "name": "Test"}]}
        mock_client.get.assert_called_once()
    
    @patch('pipeline.fetcher.httpx.Client')
    def test_fetch_page_http_error(self, mock_client_class):
        """Test d'une erreur HTTP."""
        # Mock de la réponse avec erreur
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        
        # Mock du client
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.get.return_value = mock_response
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "Not Found", request=Mock(), response=mock_response
        )
        mock_client_class.return_value = mock_client
        
        # Test
        with pytest.raises(httpx.HTTPStatusError):
            fetch_page("/search", {"page": 1})


class TestFetchAllData:
    """Tests pour la fonction fetch_all_data."""
    
    @patch('pipeline.fetcher.fetch_page')
    @patch('pipeline.fetcher.time.sleep')
    def test_fetch_all_data_success(self, mock_sleep, mock_fetch_page):
        """Test d'une récupération complète réussie."""
        # Mock des réponses paginées - première page avec données, autres vides pour arrêter
        mock_fetch_page.side_effect = [
            {"products": [{"id": i} for i in range(10)]},  # Page 1
            {"products": [{"id": i} for i in range(10, 20)]},  # Page 2
            {"products": [{"id": i} for i in range(20, 30)]},  # Page 3
            {"products": []}  # Page 4 vide pour arrêter
        ]
        
        # Test
        result = fetch_all_data("chocolats")
        
        # On devrait avoir 30 produits des 3 premières pages
        assert len(result) == 30
        # fetch_page devrait être appelé au moins 3 fois
        assert mock_fetch_page.call_count >= 3
    
    @patch('pipeline.fetcher.fetch_page')
    @patch('pipeline.fetcher.time.sleep')
    def test_fetch_all_data_empty_page(self, mock_sleep, mock_fetch_page):
        """Test avec une page vide (arrêt de la pagination)."""
        # Mock : première page avec données, deuxième vide
        mock_fetch_page.side_effect = [
            {"products": [{"id": 1}]},
            {"products": []}
        ]
        
        # Test
        result = fetch_all_data("chocolats")
        
        assert len(result) == 1
        assert mock_fetch_page.call_count == 2
    
    @patch('pipeline.fetcher.fetch_page')
    @patch('pipeline.fetcher.time.sleep')
    def test_fetch_all_data_no_data(self, mock_sleep, mock_fetch_page):
        """Test avec aucune donnée récupérée."""
        # Mock : toutes les pages vides
        mock_fetch_page.side_effect = [
            {"products": []} for _ in range(3)
        ]
        
        # Test
        with pytest.raises(ValueError, match="Aucune donnée récupérée"):
            fetch_all_data("chocolats")

